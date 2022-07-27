import signalfx
import logging
import yaml
import sys

with open("config.yaml", "r") as ymlfile:
    cfg = yaml.safe_load(ymlfile)

sfx = signalfx.SignalFx(
    api_endpoint="https://api." + cfg["REALM"] + ".signalfx.com",
    ingest_endpoint="https://ingest." + cfg["REALM"] + ".signalfx.com",
    stream_endpoint="https://stream." + cfg["REALM"] + ".signalfx.com",
)

ingest = sfx.ingest(cfg["ACCESS_TOKEN"])

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

def set_dimensions(driver, track_id):
    dimensions["driver"] = driver
    dimensions["track"] = tracks.get(track_id)


def write_telemetry_data(car_telemetry_data):
    #logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    ingest.send(
        gauges=[
            {
                "metric": "f1_2021.speed",
                "value": car_telemetry_data.m_speed,
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.engineRPM",
                "value": car_telemetry_data.m_engine_rpm,
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.gear",
                "value": car_telemetry_data.m_gear,
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.brake",
                "value": car_telemetry_data.m_brake,
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.brakeTempFL",
                "value": car_telemetry_data.m_brakes_temperature[0],
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.brakeTempFR",
                "value": car_telemetry_data.m_brakes_temperature[1],
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.brakeTempRL",
                "value": car_telemetry_data.m_brakes_temperature[2],
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.brakeTempRR",
                "value": car_telemetry_data.m_brakes_temperature[3],
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.tyresSurfaceTempFL",
                "value": car_telemetry_data.m_tyres_surface_temperature[0],
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.tyresSurfaceTempFR",
                "value": car_telemetry_data.m_tyres_surface_temperature[1],
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.tyresSurfaceTempRL",
                "value": car_telemetry_data.m_tyres_surface_temperature[2],
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.tyresSurfaceTempRR",
                "value": car_telemetry_data.m_tyres_surface_temperature[3],
                "dimensions": (dimensions),
            },
        ]
    )

    if car_telemetry_data.m_drs == 1:
        ingest.send_event(
            event_type="m_drs_enabled",
            category="USER_DEFINED",
            dimensions=(dimensions),
        )


def write_lap_data(m_lap_data):
    # logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    ingest.send(
        gauges=[
            {
                "metric": "f1_2021.currentLapNum",
                "value": m_lap_data.m_current_lap_num,
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.lastLapTime",
                "value": m_lap_data.m_last_lap_time_in_ms,
                "dimensions": (dimensions),
            },
            {
                "metric": "f1_2021.currentLapTime",
                "value": m_lap_data.m_current_lap_time_in_ms,
                "dimensions": (dimensions),
            },
        ]
    )
