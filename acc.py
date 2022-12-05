#!/usr/bin/env python3
from pyaccsharedmemory import accSharedMemory  # type: ignore
import time
import sys
import requests  # type: ignore
from requests.adapters import HTTPAdapter  # type: ignore
import json
import configparser
import signalfx  # type: ignore
import urllib3  # type: ignore
import background  # type: ignore
import enum
from rich.panel import Panel  # type: ignore
from rich.layout import Layout  # type: ignore
from rich.live import Live  # type: ignore
from rich.prompt import Prompt  # type: ignore
import argparse

layout = Layout()

urllib3.disable_warnings()
background.n = 100

# mode should be 'spectator' to grab all cars, 'solo' to only grab data for the player car
parser = argparse.ArgumentParser(description="ACC - Splunk Data Drivers")
parser.add_argument("--hostname", help="Hostname", default="rig_1")
parser.add_argument("-e", "--endpoint", help="Send data to Splunk Core, O11y Cloud or both", choices=["core", "o11y", "both"], default="both")
args = vars(parser.parse_args())

hostname = args["hostname"]
endpoint = args["endpoint"]

physics_dict = {}
graphics_dict = {}
static_dict = {}

config = configparser.ConfigParser()
config.read("settings.ini")

# Set Debugging
debug = config.getboolean("ingest", "debug")
# Splunk Enterprise Variables
splunk_hec_ip = config.get("ingest", "splunk_hec_ip")
splunk_hec_port = config.get("ingest", "splunk_hec_port")
splunk_hec_token = config.get("ingest", "splunk_hec_token")

# SIM variables
sim_token = config.get("ingest", "sim_token")
sim_endpoint = config.get("ingest", "ingest_endpoint")

# Telemetry
physics = config.getboolean("telemetry", "physics")
graphics = config.getboolean("telemetry", "graphics")
static = config.getboolean("telemetry", "static")

sim_metrics = []

for key, metric in config.items("sim_metrics"):
    sim_metrics.append(metric)

client = signalfx.SignalFx(ingest_endpoint=sim_endpoint)
ingest = client.ingest(sim_token)

# create session object for http keep alives
sesh = requests.Session()
sesh.mount("https://", HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=0, pool_block=False))

name = Prompt.ask("[b green]Please enter your name[/b green]")

layout.split_column(Layout(name="upper", size=9), Layout(name="lower"))
layout["upper"].split_row(
    Layout(name="top_left"),
    Layout(name="top_right"),
)

layout["top_left"].update(Layout(Panel(f"\n{name}", title="[b cyan]Current Player", title_align="left")))

layout["lower"].update(
    Layout(
        Panel(
            f"\nSplunk HEC Endpoint:  [bright_cyan]{splunk_hec_ip}:{splunk_hec_port}[/bright_cyan]\n\n"
            + f"Splunk O11y Endpoint: [bright_cyan]{sim_endpoint}[/bright_cyan]\n\n"
            + f"Core, O11y or both: [b bright_green]{endpoint.capitalize()}[/b bright_green]\n"
            + f"Physics:  {'[bright_green]True[/bright_green]' if physics else '[i red]False[/i red]'}\n"
            + f"Graphics: {'[bright_green]True[/bright_green]' if graphics else '[i red]False[/i red]'}\n"
            + f"Static:   {'[bright_green]True[/bright_green]' if static else '[i red]False[/i red]'}\n",
            title="[b cyan]Configuration",
            title_align="left",
        )
    )
)

layout["top_right"].update(Layout(Panel("\n[i bright_red]Awaiting Player data ...[/i bright_red]", title="[b cyan]Telemetry", title_align="left")))


def send_metrics(data, sourcetype):
    telemetry_json = []

    dimensions = {"player_name": name, "sourcetype": sourcetype}

    for k in data:
        for key in sim_metrics:
            if key in k:
                telemetry_json.append({"metric": "acc." + k, "value": data[key], "dimensions": dimensions})

    ingest.send(gauges=telemetry_json)


@background.task
def send_hec(data, sourcetype):
    #event = {}
    #event["host"] = hostname
    #event["sourcetype"] = sourcetype
    #event["source"] = "acc"
    #event["time"] = int(time.time_ns() / 1000)
    #event["event"] = data

    event = {'host': hostname,
             'sourcetype': sourcetype,
             'source': 'acc',
             'time': int(time.time_ns() / 1000),
             'event': data}

    url = splunk_hec_ip + ":" + splunk_hec_port + "/services/collector"
    header = {"Authorization": "Splunk " + splunk_hec_token}

    try:
        response = sesh.post(url=url, data=json.dumps(event), headers=header, verify=False)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        layout["top_right"].update(Layout(Panel("\n[i bright_red]Awaiting Player data ...[/i bright_red]", title="[b cyan]Telemetry", title_align="left")))


def get_physics_data(data):
    for attr, val in data.__dict__.items():
        if type(val).__name__ == "Wheels":
            for w, v in val.__dict__.items():
                physics_dict.update({attr + "_" + w: v})
        elif type(val).__name__ == "CarDamage":
            for w, v in val.__dict__.items():
                physics_dict.update({attr + "_" + w: v})
        elif type(val).__name__ == "Vector3f":
            for w, v in val.__dict__.items():
                physics_dict.update({attr + "_" + w: v})
        elif type(val).__name__ == "ContactPoint":
            for w, v in val.__dict__.items():
                for x, y in v.__dict__.items():
                    physics_dict.update({attr + "_" + w + "_" + x: y})
        else:
            physics_dict.update({attr: val})
    
    physics_dict.update({"driver_name": name})
    
    return physics_dict


def get_graphics_data(data):
    counter = -1
    for attr, val in data.__dict__.items():
        if isinstance(val, str):
            graphics_dict.update({attr: val.rstrip("\x00")})
        elif attr == "car_coordinates":
            for c in val:
                coords = {}
                for v, b in c.__dict__.items():
                    coords.update({v: b})
                counter += 1
                graphics_dict.update({attr + "_" + str(counter): coords})
        elif type(val).__name__ == "Wheels":
            for w, v in val.__dict__.items():
                graphics_dict.update({attr + "_" + w: v})
        elif isinstance(val, enum.Enum):
            pass
        elif attr == "car_id":
            pass
        else:
            graphics_dict.update({attr: val})

    graphics_dict.update({"driver_name": name})

    return graphics_dict


def get_static_data(data):
    for attr, val in data.__dict__.items():
        if isinstance(val, str):
            static_dict.update({attr: val.rstrip("\x00")})
        else:
            static_dict.update({attr: val})

    static_dict.update({"driver_name": name})

    return static_dict


def update_console(rpm, kmh, gear, brake, gas):
    kmh = round(kmh, 1)
    gear = gear - 1
    brake = brake * 100
    gas = gas * 100

    layout["top_right"].update(
        Layout(
            Panel(
                f"\n"
                + f"Current Speed: [yellow]{kmh} kph[/yellow]\n"
                + f"Engine RPM:    [yellow]{rpm} rpm[/yellow]\n"
                + f"Current Gear:  [green_yellow]{gear}[/green_yellow]\n"
                + f"Brake:    [dark_orange]{brake:.0f}%[/dark_orange]\n"
                + f"Throttle: [dark_orange]{gas:.0f}%[/dark_orange]",
                title="[b cyan]Telemetry[/b cyan]",
                title_align="left",
            )
        )
    )


@background.task
def process_data(data, sourcetype):
    if sourcetype == "ACC_Physics":
        json_payload = get_physics_data(data)

        update_console(data.rpm, data.speed_kmh, data.gear, data.brake, data.gas)

    if sourcetype == "ACC_Graphics":
        json_payload = get_graphics_data(data)

    if sourcetype == "ACC_Static":
        json_payload = get_static_data(data)

    if args["endpoint"] == "core" or args["endpoint"] == "both":
        send_hec(json_payload, sourcetype)

    if args["endpoint"] == "o11y" or args["endpoint"] == "both":
        send_metrics(json_payload, sourcetype)


if __name__ == "__main__":
    with Live(layout, refresh_per_second = 0.5) as live:
        try:
            while True:
                asm = accSharedMemory()
                sm = asm.read_shared_memory()
                if not (isinstance(sm, type(None))):

                    if static is True:
                        process_data(sm.Static, "ACC_Static")

                    if graphics is True:
                        process_data(sm.Graphics, "ACC_Graphics")

                    if physics is True:
                        process_data(sm.Physics, "ACC_Physics")
                    
                    time.sleep(0.1)
                else:
                    asm.close()
                    sys.exit("No ACC metrics detected!")
        except KeyboardInterrupt:
            asm.close()
