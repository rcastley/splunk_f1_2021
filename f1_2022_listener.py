#!/usr/bin/env python3
import os
import time
import signalfx # type: ignore
import configparser
import argparse
import json
import requests # type: ignore
import background # type: ignore
import urllib3 # type: ignore
from datetime import datetime
from f1_22_telemetry.listener import TelemetryListener # type: ignore
from requests.adapters import HTTPAdapter # type: ignore
from rich import print # type: ignore
from rich.panel import Panel # type: ignore
from rich.layout import Layout # type: ignore
from rich.live import Live # type: ignore
import sqlite3

layout = Layout()

urllib3.disable_warnings()
background.n = 100

database = "players.sqlite"

# mode should be 'spectator' to grab all cars, 'solo' to only grab data for the player car
parser = argparse.ArgumentParser(description="F1 2022 - Splunk Data Drivers")
parser.add_argument("--hostname", help="Hostname", default="rig_1")
parser.add_argument("-e", "--endpoint", help="Send data to Splunk Core, O11y Cloud or both", choices=["core", "o11y", "both"], default="both")
parser.add_argument("-m", "--mode", help="Spectator or Solo Mode", choices=["spectator", "solo"], default="solo")
args = vars(parser.parse_args())

hostname = args["hostname"]

conn = sqlite3.connect(database, check_same_thread=False)
get_players = conn.execute("SELECT player_name, hostname, port FROM players WHERE hostname = ?", (hostname,))
rows = get_players.fetchone()
conn.close()

#conn.execute("UPDATE players SET listener_pid = ? WHERE hostname = ?", (1, hostname))
#conn.commit()

player_name = rows[0]
mode = args["mode"]
udp_port = int(rows[2])
endpoint = args["endpoint"]

sesh = requests.Session()
# retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
sesh.mount("https://", HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=0, pool_block=False))

# Open config file for read
config = configparser.ConfigParser()
config.read("settings.ini")
# Set Debugging
debug = config.getboolean("ingest", "debug")
# Splunk Cloud/Enterprise Variables
splunk_hec_ip = config.get("ingest", "splunk_hec_ip")
splunk_hec_port = config.get("ingest", "splunk_hec_port")
splunk_hec_token = config.get("ingest", "splunk_hec_token")
# O11y Cloud variables
sim_token = config.get("ingest", "sim_token")
sim_endpoint = config.get("ingest", "ingest_endpoint")
# Telemetry varilables
car_motion = config.getboolean("telemetry", "car_motion")
car_telemetry = config.getboolean("telemetry", "car_telemetry")
lap_data = config.getboolean("telemetry", "lap_data")
car_status = config.getboolean("telemetry", "car_status")
car_setup = config.getboolean("telemetry", "car_setup")

sim_metrics = []

#for key, metric in config.items("sim_metrics"):
#    sim_metrics.append(metric)

sim_metrics = [metric for metric in config["sim_metrics"].values()]

packet_dict = {}

for key, packet_ids in config.items("packet_ids"):
    packet_dict[int(key)] = packet_ids

client = signalfx.SignalFx(ingest_endpoint=sim_endpoint)
ingest = client.ingest(sim_token)

layout.split_column(Layout(name="upper", size=9), Layout(name="lower"))
layout["upper"].split_row(
    Layout(name="top_left"),
    Layout(name="top_right"),
)

layout["top_left"].update(Layout(Panel(f"\nHostname: [b yellow]{hostname}[/b yellow]\n\nUDP Port: [b orange_red1]{udp_port}[/b orange_red1]\n\nPlayer Name: [magenta1]{player_name}", title="[b cyan]Current Player", title_align="left")))

layout["lower"].update(
    Layout(
        Panel(
            f"\nSplunk HEC Endpoint:  [bright_cyan]{splunk_hec_ip}:{splunk_hec_port}[/bright_cyan]\n\nSplunk O11y Endpoint: [bright_cyan]{sim_endpoint}[/bright_cyan]\n\n"
            + f"Core, O11y or both:  [b bright_green]{endpoint.capitalize()}[/b bright_green]\n"
            + f"Solo or Spectator:   [b bright_green]{mode.capitalize()}[/b bright_green]\n\n"
            + f"Car Telemetry: {'[bright_green]True[/bright_green]' if car_telemetry else '[i red]False[/i ed]'}\n"
            + f"Car Motion:    {'[bright_green]True[/bright_green]' if car_motion else '[i red]False[/i red]'}\n"
            + f"Car Setup:     {'[bright_green]True[/bright_green]' if car_setup else '[i red]False[/i red]'}\n"
            + f"Car Status:    {'[bright_green]True[/bright_green]' if car_status else '[i red]False[/i red]'}\n"
            + f"Lap Data:      {'[bright_green]True[/bright_green]' if lap_data else '[i red]False[/i red]'}",
            title="[b cyan]Configuration",
            title_align="left",
        )
    )
)

layout["top_right"].update(Layout(Panel("\n[i bright_red]Awaiting Player data ...[/i bright_red]", title="[b cyan]Telemetry", title_align="left")))

#########################################
# Set up global variables and data stores

player_info = [{"ai_controlled": 1, "driver_id": 63, "name": "", "nationality": 13, "race_number": 6, "team_id": 3, "your_telemetry": 1}]

lap_info = []

for i in range(1, 21):
    lap_info = lap_info + [{"current_sector": 0, "current_lap": 1, "lap_event": "none", "lap_event_count": 0}]

sim_dimensions = ["name", "player_name"]


def lookup_packet_id(packet_id):
    return packet_dict[packet_id]


# Sends metrics to O11y Cloud
def send_metric(f1_metrics, f1_dimensions):
    telemetry_json = []
    f1_dimensions["f1-2022-hostname"] = hostname

    for key, value in f1_metrics.items():
        telemetry_json.append({"metric": "f1_2022." + key, "value": value, "dimensions": f1_dimensions})

    ingest.send(gauges=telemetry_json)


def send_dims_and_metrics(f1_json):
    dimensions = [{key: car_dict[key] for key in sim_dimensions if key in car_dict} for car_dict in f1_json]

    metrics = [{key: car_dict[key] for key in sim_metrics if key in car_dict} for car_dict in f1_json]

    for f1_metrics, f1_dimensions in zip(metrics, dimensions):
        if len(f1_metrics) >= 1:
            # Send current row to O11y Cloud
            send_metric(f1_metrics, f1_dimensions)


# Function to send raw unprocessed event to HEC
@background.task
def send_hec_json(data, packet_id):
    hec_payload = ""

    event = {}
    event["time"] = datetime.now().timestamp()
    event["sourcetype"] = lookup_packet_id(packet_id)
    event["source"] = "f1_2022"
    event["host"] = hostname
    event["event"] = data

    hec_payload = hec_payload + json.dumps(event)

    url = splunk_hec_ip + ":" + splunk_hec_port + "/services/collector"
    header = {"Authorization": "Splunk " + splunk_hec_token}

    try:
        response = sesh.post(url=url, data=hec_payload, headers=header, verify=False)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(err)
        print("Packet ID affected: " + lookup_packet_id(packet_id))


# function to send multiple events to splunk enterprise env
@background.task
def send_hec_batch(event_rows, packet_id):
    event_rows = [{key: str(dict[key]) for key in dict.keys()} for dict in event_rows]

    hec_payload = ""

    for row in event_rows:
        event = {}
        event["time"] = datetime.now().timestamp()
        event["sourcetype"] = lookup_packet_id(packet_id)
        event["source"] = "f1_2022"
        event["host"] = hostname
        event["event"] = row

        hec_payload = hec_payload + json.dumps(event)

    url = splunk_hec_ip + ":" + splunk_hec_port + "/services/collector"
    header = {"Authorization": "Splunk " + splunk_hec_token}

    try:
        response = sesh.post(url=url, data=hec_payload, headers=header, verify=False)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        print(err)
        print("Packet ID affected: " + lookup_packet_id(packet_id))


#########################################
# Data Stream Management and Processing
def update_player_info(data):
    global player_info
    player_info = data["participants"]


def update_console(rpm, kph, gear, brake, throttle):
    global player_name
    brake = brake * 100
    throttle = throttle * 100
    if gear == -1:
        gear = "Reverse"
    layout["top_right"].update(
        Layout(
            Panel(
                f"\n"
                + f"Engine RPM:    [yellow]{rpm} rpm[/yellow]\n"
                + f"Current Speed: [yellow]{kph} kph[/yellow]\n"
                + f"Current Gear:  [green_yellow]{gear}[/green_yellow]\n"
                + f"Brake:    [dark_orange]{brake:.0f}%[/dark_orange]\n"
                + f"Throttle: [dark_orange]{throttle:.0f}%[/dark_orange]",
                title="[b cyan]Telemetry[/b cyan]",
                title_align="left",
            )
        )
    )
    conn = sqlite3.connect(database, check_same_thread=False)
    get_players = conn.execute("SELECT player_name, hostname, port FROM players WHERE hostname = ?", (hostname,))
    rows = get_players.fetchone()
    player_name = rows[0]
    layout["top_left"].update(Layout(Panel(f"\nHostname: [b yellow]{hostname}[/b yellow]\n\nUDP Port: [b orange_red1]{udp_port}[/b orange_red1]\n\nPlayer Name: [magenta1]{player_name}", title="[b cyan]Current Player", title_align="left")))
    conn.execute("UPDATE players SET throttle = ?, speed = ?, brake = ? WHERE hostname = ?", (int(throttle), kph, int(brake), hostname))
    conn.commit()
    conn.close()

    return


# Flatten packet format
def flatten_data(data):
    blank_list = []
    car_index = 0
    for entry in data:
        blank_dict = {}
        blank_dict.update({element: entry[element] for element in entry if not isinstance(entry[element], list)})
        multi_value_fields = {element: {element + str(ind + 1): value for ind, value in enumerate(entry[element])} for element in entry if isinstance(entry[element], list)}

        for field in multi_value_fields:
            blank_dict.update(multi_value_fields[field])

        blank_dict.update({"car_index": car_index})
        car_index += 1
        blank_list = blank_list + [blank_dict]

    telemetry = blank_list

    return telemetry


def set_mode_data(telemetry, playerCarIndex):
    # If not in spectator mode, get rid of the non-player cars
    if mode == "solo":
        # get only player car from flattened data
        data = [telemetry[playerCarIndex]]
        data[0].update({"player_name": player_name})
    else:
        data = telemetry

    return data


def augment_packet(header, telemetry, data, *args):
    # Augment with header and player info data
    for entry, player in zip(telemetry, player_info):
        entry.update(player)
        entry.update(header)
        # Augent with additional args
        for a in args:
            entry.update({a: data[a]})

    return telemetry


def merge_car_telemetry(data, header, playerCarIndex):
    telemetry = flatten_data(data["car_telemetry_data"])

    augmented_telemetry = augment_packet(header, telemetry, data)

    return set_mode_data(augmented_telemetry, playerCarIndex)


def merge_car_setups(data, header, playerCarIndex):
    telemetry = flatten_data(data["car_setups"])

    augmented_telemetry = augment_packet(header, telemetry, data)

    return set_mode_data(augmented_telemetry, playerCarIndex)


def merge_car_status(data, header, playerCarIndex):
    telemetry = flatten_data(data["car_status_data"])

    augmented_telemetry = augment_packet(header, telemetry, data)

    return set_mode_data(augmented_telemetry, playerCarIndex)


def merge_session_history(data, header, playerCarIndex):
    telemetry = flatten_data(data["lap_history_data"])

    augmented_telemetry = augment_packet(header, telemetry, data)

    return set_mode_data(augmented_telemetry, playerCarIndex)


def merge_session(data, header, playerCarIndex):
    telemetry = flatten_data(data["marshal_zones"])

    augmented_telemetry = augment_packet(header, telemetry, data, "air_temperature", "track_id", "weather", "total_laps", "track_temperature", "track_length")

    return set_mode_data(augmented_telemetry, playerCarIndex)


def merge_final(data, header, playerCarIndex):
    telemetry = flatten_data(data["classification_data"])

    augmented_telemetry = augment_packet(header, telemetry, data, "num_cars")

    return set_mode_data(augmented_telemetry, playerCarIndex)


def merge_lobby(data, header, playerCarIndex):
    telemetry = flatten_data(data["lobby_players"])

    augmented_telemetry = augment_packet(header, telemetry, data, "num_players")

    return set_mode_data(augmented_telemetry, playerCarIndex)


def merge_car_motion(data, header, playerCarIndex):
    telemetry = flatten_data(data["car_motion_data"])

    # Augment with header and player info data
    for entry, player in zip(telemetry, player_info):
        entry.update(player)
        entry.update(header)

    # If not in spectator mode, get rid of the non-player cars
    if mode != "spectator":
        # get only player car from flattened data
        telemetry = [telemetry[playerCarIndex]]
        telemetry[0].update({"player_name": player_name})
        # Get additional motion data, not stored in the main array
        # Get the per-wheel motion data for the player car
        player_car_motion_list = [
            "suspension_acceleration",
            "suspension_position",
            "suspension_velocity",
            "wheel_slip",
            "wheel_speed",
        ]
        for motion_data in player_car_motion_list:
            i = 1
            for value in data[motion_data]:
                telemetry[0].update({motion_data + str(i): value})
                i += 1

    return telemetry


def merge_car_lap(data, header, playerCarIndex):
    telemetry = flatten_data(data["lap_data"])

    # Augment with header and player info data
    for entry, player in zip(telemetry, player_info):
        entry.update(player)
        entry.update(header)

    # check for events such as lap or sector completeion
    for entry, info_buffer in zip(telemetry, lap_info):
        if info_buffer["current_lap"] < entry["current_lap_num"]:
            info_buffer.update({"lap_event": "LAP_COMPLETE", "lap_event_count": 0})
            entry.update({"lap_event": "LAP_COMPLETE"})

        if info_buffer["current_sector"] < entry["sector"]:
            info_buffer.update({"lap_event": "SECTOR_COMPLETE", "lap_event_count": 0})
            entry.update({"lap_event": "SECTOR_COMPLETE"})

        # repeat event anouncement for 3 seconds in case of network loss
        if info_buffer["lap_event"] != "none":
            if info_buffer["lap_event_count"] < 5:
                entry.update(
                    {
                        "lap_event": info_buffer["lap_event"],
                        "lap_event_count": info_buffer["lap_event_count"],
                    }
                )

                info_buffer.update({"lap_event_count": info_buffer["lap_event_count"] + 1})
            else:
                entry.update({"lap_event": "none"})
                info_buffer.update({"lap_event": "none"})
                info_buffer.update({"lap_event_count": 0})

        info_buffer.update({"current_sector": entry["sector"], "current_lap": entry["current_lap_num"]})
        entry.update({"lap_event": info_buffer["lap_event"]})

    return set_mode_data(telemetry, playerCarIndex)


def send_augmented_json(data, packet_id):
    data.update({"player_name": player_name})
    send_hec_json(data, packet_id)


@background.task
def massage_data(data):
    dict_object = data.to_json()
    data = json.loads(dict_object)
    packet_id = data["header"]["packet_id"]
    header = data["header"]
    playerCarIndex = data["header"]["player_car_index"]

    if debug == True:
        data["header"].update({"checkpoint_1": time.time()})

    if packet_id == 0:
        if car_motion == True:
            merged_data = merge_car_motion(data, header, playerCarIndex)
        else:
            return

    if packet_id == 1:
        merged_data = merge_session(data, header, playerCarIndex)

    if packet_id == 2:
        if lap_data == True:
            merged_data = merge_car_lap(data, header, playerCarIndex)
        else:
            return

    if packet_id == 3:
        try:
            if args["endpoint"] == "core" or args["endpoint"] == "both":
                send_augmented_json(data, packet_id)
        except Exception as e:
            print(str(e))
        return

    if packet_id == 4:
        update_player_info(data)

    if packet_id == 5:
        if car_setup == True:
            merged_data = merge_car_setups(data, header, playerCarIndex)
        else:
            return

    if packet_id == 6:
        if car_telemetry == True:
            merged_data = merge_car_telemetry(data, header, playerCarIndex)
            update_console(merged_data[0]["engine_rpm"], merged_data[0]["speed"], merged_data[0]["gear"], merged_data[0]["brake"], merged_data[0]["throttle"])
        else:
            return

    if packet_id == 7:
        if car_status == True:
            merged_data = merge_car_status(data, header, playerCarIndex)
        else:
            return

    if packet_id == 8:
        merged_data = merge_final(data, header, playerCarIndex)

    if packet_id == 9:
        merged_data = merge_lobby(data, header, playerCarIndex)

    if packet_id == 11:

        merged_data = merge_session_history(data, header, playerCarIndex)

    merged_data = [entry for entry in merged_data if entry["name"] != ""]

    if debug == True:
        for entry in merged_data:
            entry.update({"checkpoint_3_payload_processed": time.time()})

    # send data to HEC
    if args["endpoint"] == "core" or args["endpoint"] == "both":
        send_hec_batch(merged_data, packet_id)

    # Send data to O11y Cloud
    if args["endpoint"] == "o11y" or args["endpoint"] == "both":
        send_dims_and_metrics(merged_data)


# Initialise session
startup_payload = [{"message": "F1 2022 script starting", "description": "Initialising Script", "splunk_hec_ip": splunk_hec_ip, "checkpoint_1": datetime.now().timestamp()}]

if args["endpoint"] == "core" or args["endpoint"] == "both":
    send_hec_batch(startup_payload, 99)

listener = TelemetryListener(port=udp_port)

with Live(layout, refresh_per_second=4) as live:
    try:
        while True:
            packet = listener.get()
            massage_data(packet)
    except KeyboardInterrupt:
        conn = sqlite3.connect(database, check_same_thread=False)
        conn.execute("UPDATE players SET listener_pid = ? WHERE hostname = ?", (0, hostname))
        conn.commit()
        conn.close()
        pass
