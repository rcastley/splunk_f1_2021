import socket
import yaml
import telemetry.splunk as metrics
from telemetry_f1_2021.listener import TelemetryListener
#from telemetry_f1_2021.packets import PacketSessionData, PacketSessionHistoryData, PacketLapData, PacketCarTelemetryData, PacketCarStatusData
from socket import gethostname, getfqdn, gethostbyname

with open("config.yaml", "r") as ymlfile:
    cfg = yaml.safe_load(ymlfile)


def start_driver(driver_name):
    listener = TelemetryListener(port=cfg['UDP_PORT'])
    print(f"Session for driver {driver_name} started ...")
    print(f"UDP Server listening to port {cfg['UDP_PORT']} on IP address: {socket.gethostbyname(socket.getfqdn())}")
    lastLap = 0  # force a new lap after we pass start finish
    sector3TimeInMS = 0

    # Receive Packages
    while True:
        packet = listener.get()
        packet_type = packet.m_header.m_packet_id  # get the packet type from the header
        position = (
            packet.m_header.m_player_car_index
        )  # get the position of the driver in the list as the packet contains data of all driving cars
       
        # TRACK ID
        if packet_type == 1:
            if packet.m_track_id is not None:
                metrics.set_dimensions(driver_name, packet.m_track_id)

        # LAP DATA
        if packet_type == 2:
            car_laptime_data = packet.m_lap_data[position]
            if car_laptime_data.m_current_lap_num > lastLap:
                lastLap = car_laptime_data.m_current_lap_num
                sector3TimeInMS = car_laptime_data.m_last_lap_time_in_ms - (
                    car_laptime_data.m_sector1_time_in_ms + car_laptime_data.m_sector2_time_in_ms
                )
                print(f"Starting Lap {lastLap}...")
            elif (
                car_laptime_data.m_sector1_time_in_ms == 0
                and car_laptime_data.m_sector2_time_in_ms == 0
            ):
                sector3TimeInMS = 0
            metrics.write_lap_data_to_splunk(
                driver_name, car_laptime_data, sector3TimeInMS
            )

        # TELEMETRY DATA
        if packet_type == 6:
            #car_telemetry_data = packet.m_car_telemetry_data[position]
            metrics.write_telemetry_data_to_splunk(driver_name, packet.m_car_telemetry_data[position])


        # CAR STATUS DATA
        #if packet_type == 7:
        #    print(packet)
        #    car_status_data = packet.m_car_status_data[position]
        #    metrics.write_car_status_data_to_splunk(driver_name, car_status_data)
