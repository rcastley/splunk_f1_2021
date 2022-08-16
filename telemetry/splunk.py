import signalfx
import logging
import yaml
import sys
import json
import requests
import time
import threading

with open("config.yaml", "r") as ymlfile:
    cfg = yaml.safe_load(ymlfile)

sfx = signalfx.SignalFx(
    api_endpoint="https://api." + cfg["SPLUNK_REALM"] + ".signalfx.com",
    ingest_endpoint="https://ingest." + cfg["SPLUNK_REALM"] + ".signalfx.com",
    stream_endpoint="https://stream." + cfg["SPLUNK_REALM"] + ".signalfx.com",
)

ingest = sfx.ingest(cfg["SPLUNK_ACCESS_TOKEN"])

url = cfg["SPLUNK_HEC_ENDPOINT"]
headers = {"Authorization": "Splunk " + cfg["SPLUNK_HEC_TOKEN"]}

tracks = {
    0: "Melbourne",
    1: "Paul Ricard",
    2: "Shanghai",
    3: "Sakhir (Bahrain)",
    4: "Catalunya",
    5: "Monaco",
    6: "Montreal",
    7: "Silverstone",
    8: "Hockenheim",
    9: "Hungaroring",
    10: "Spa",
    11: "Monza",
    12: "Singapore",
    13: "Suzuka",
    14: "Abu Dhabi",
    15: "Texas",
    16: "Brazil",
    17: "Austria",
    18: "Sochi",
    19: "Mexico",
    20: "Baku (Azerbaijan)",
    21: "Sakhir Short",
    22: "Silverstone Short",
    23: "Texas Short",
    24: "Suzuka Short",
    25: "Hanoi",
    26: "Zandvoort",
    27: "Imola",
    28: "Portimão",
    29: "Jeddah",
}

dimensions = {}

def set_dimensions(session_uid, driver_name, track_id):
    dimensions["driver"] = driver_name
    dimensions["track"] = tracks.get(track_id)


def write_temperatures(track_temperature, air_temperature):
    #logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    splunk_temperature_json = {}

    splunk_temperature_json['metric_name:f1_2021.trackTemp'] = track_temperature
    splunk_temperature_json['metric_name:f1_2021.airTemp'] = air_temperature

    temperature_json = [{
            "metric": "f1_2021.trackTemp",
            "value": track_temperature,
            "dimensions": dimensions,
        },{
           "metric": "f1_2021.airTemp",
            "value": air_temperature,
            "dimensions": dimensions,
        }]
    
    temps = threading.Thread(target=write_splunk_hec, name="splunk_temperature", args=(splunk_temperature_json,))
    temps.start()

    ingest.send(gauges=temperature_json)


def write_telemetry_data(car_telemetry_data):
    #logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    splunk_telemetry_json = {}
    splunk_telemetry_json['metric_name:f1_2021.speed'] = car_telemetry_data.m_speed
    splunk_telemetry_json['metric_name:f1_2021.engineRPM'] = car_telemetry_data.m_engine_rpm


    telemetry_json = [
        {
            "metric": "f1_2021.speed",
            "value": car_telemetry_data.m_speed,
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.engineRPM",
            "value": car_telemetry_data.m_engine_rpm,
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.gear",
            "value": car_telemetry_data.m_gear,
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.brake",
            "value": car_telemetry_data.m_brake,
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.brakeTempFL",
            "value": car_telemetry_data.m_brakes_temperature[0],
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.brakeTempFR",
            "value": car_telemetry_data.m_brakes_temperature[1],
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.brakeTempRL",
            "value": car_telemetry_data.m_brakes_temperature[2],
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.brakeTempRR",
            "value": car_telemetry_data.m_brakes_temperature[3],
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.tyresSurfaceTempFL",
            "value": car_telemetry_data.m_tyres_surface_temperature[0],
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.tyresSurfaceTempFR",
            "value": car_telemetry_data.m_tyres_surface_temperature[1],
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.tyresSurfaceTempRL",
            "value": car_telemetry_data.m_tyres_surface_temperature[2],
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.tyresSurfaceTempRR",
            "value": car_telemetry_data.m_tyres_surface_temperature[3],
            "dimensions": dimensions,
        },
    ]

    telemetry = threading.Thread(target=write_splunk_hec, name="splunk_telemetry", args=(splunk_telemetry_json,))
    telemetry.start()

    #for thread in threading.enumerate(): 
    #    print(thread.name)

    ingest.send(gauges=telemetry_json)

    if car_telemetry_data.m_drs == 1:
        ingest.send_event(
            event_type="m_drs_enabled",
            category="USER_DEFINED",
            dimensions=dimensions,
        )


def write_lap_data(m_lap_data):
    #logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    lap_data_json = [
        {
            "metric": "f1_2021.currentLapNum",
            "value": m_lap_data.m_current_lap_num,
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.lastLapTime",
            "value": m_lap_data.m_last_lap_time_in_ms,
            "dimensions": dimensions,
        },
        {
            "metric": "f1_2021.currentLapTime",
            "value": m_lap_data.m_current_lap_time_in_ms,
            "dimensions": dimensions,
        },
    ]

    # write_splunk_hec(lap_data_json)

    ingest.send(gauges=lap_data_json)


def write_splunk_hec(json_data):
    event = {}
    event["time"] = int(time.time())
    event["event"] = "metric"
    event["sourcetype"] = "f1_2021_telemetry"
    event["host"] = "xbox"
    event["source"] = "metrics"
    event["fields"] = json_data
    payload = json.dumps(event, separators=(',', ':'))
    resp = requests.post(url, data=payload, headers=headers)
