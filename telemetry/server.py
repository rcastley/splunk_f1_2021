import socket
import yaml
from f1_2020_telemetry.packets import unpack_udp_packet

import telemetry.splunk as metrics

with open("config.yaml", "r") as ymlfile:
    cfg = yaml.safe_load(ymlfile)

def start_driver(driver_name):
    print(f"driver {driver_name} started...")
    
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    udp_socket.bind(("", cfg['UDP_PORT']))
    print(f"Listening to Port {cfg['UDP_PORT']}...")

    # Receive Packages
    while True:
        udp_packet = udp_socket.recv(2048)
        packet = unpack_udp_packet(udp_packet)

        packet_type = packet.header.packetId # get the packet type from the header
        position = packet.header.playerCarIndex # get the position of the driver in the list as the packet contains data of all driving cars
        
        # LAP DATA
        if packet_type == 2:
            car_laptime_data = packet.lapData[position]
            metrics.write_lap_data_to_splunk(driver_name, car_laptime_data)            

        # TELEMETRY DATA
        if packet_type == 6:
            car_telemetry_data = packet.carTelemetryData[position]
            metrics.write_telemetry_data_to_splunk(driver_name, car_telemetry_data)
