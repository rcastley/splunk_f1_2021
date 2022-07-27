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


def write_telemetry_data_to_splunk(driver_name, car_telemetry_data):
    # logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    ingest.send(
        gauges=[
            {
                "metric": "f1_2021.speed",
                "value": car_telemetry_data.speed,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.engineRPM",
                "value": car_telemetry_data.engineRPM,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.gear",
                "value": car_telemetry_data.gear,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.brake",
                "value": car_telemetry_data.brake,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.brakeTempFL",
                "value": car_telemetry_data.brakesTemperature[0],
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.brakeTempFR",
                "value": car_telemetry_data.brakesTemperature[1],
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.brakeTempRL",
                "value": car_telemetry_data.brakesTemperature[2],
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.brakeTempRR",
                "value": car_telemetry_data.brakesTemperature[3],
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.tyresSurfaceTempFL",
                "value": car_telemetry_data.tyresSurfaceTemperature[0],
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.tyresSurfaceTempFR",
                "value": car_telemetry_data.tyresSurfaceTemperature[1],
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.tyresSurfaceTempRL",
                "value": car_telemetry_data.tyresSurfaceTemperature[2],
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.tyresSurfaceTempRR",
                "value": car_telemetry_data.tyresSurfaceTemperature[3],
                "dimensions": {"driver": driver_name},
            },
        ]
    )

    if car_telemetry_data.drs == 1:
        ingest.send_event(
            event_type="drs_enabled",
            category="USER_DEFINED",
            dimensions={"driver": driver_name},
        )


def write_car_status_data_to_splunk(driver_name, car_status_data):
    # logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    ingest.send(
        gauges=[
            {
                "metric": "f1_2021.tyreWearFL",
                "value": car_status_data.tyresWear[0],
                "dimensions": {"driver": driver_name},
            },
        ]
    )


def write_lap_data_to_splunk(driver_name, car_laptime_data, sector3TimeInS):
    # logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    ingest.send(
        gauges=[
            {
                "metric": "f1_2021.currentLapNum",
                "value": car_laptime_data.currentLapNum,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.lastLapTime",
                "value": car_laptime_data.lastLapTime,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.currentLapTime",
                "value": car_laptime_data.currentLapTime,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.bestLapTime",
                "value": car_laptime_data.bestLapTime,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.bestLapNum",
                "value": car_laptime_data.bestLapNum,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.sector1TimeInMS",
                "value": car_laptime_data.sector1TimeInMS,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.sector2TimeInMS",
                "value": car_laptime_data.sector2TimeInMS,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.bestOverallSector1TimeInMS",
                "value": car_laptime_data.bestOverallSector1TimeInMS,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.bestOverallSector1LapNum",
                "value": car_laptime_data.bestOverallSector2LapNum,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.bestOverallSector2TimeInMS",
                "value": car_laptime_data.bestOverallSector2TimeInMS,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.bestOverallSector2LapNum",
                "value": car_laptime_data.bestOverallSector2LapNum,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.bestOverallSector3TimeInMS",
                "value": car_laptime_data.bestOverallSector3TimeInMS,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.bestOverallSector3LapNum",
                "value": car_laptime_data.bestOverallSector2LapNum,
                "dimensions": {"driver": driver_name},
            },
            {
                "metric": "f1_2021.sector3TimeInS",
                "value": sector3TimeInS,
                "dimensions": {"driver": driver_name},
            },
        ]
    )
