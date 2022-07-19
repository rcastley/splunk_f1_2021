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
    lastLap = 0  # force a new lap after we pass start finish
    sector3time = 0    
    # Receive Packages
    while True:
        udp_packet = udp_socket.recv(2048)
        packet = unpack_udp_packet(udp_packet)

        packet_type = packet.header.packetId # get the packet type from the header
        position = packet.header.playerCarIndex # get the position of the driver in the list as the packet contains data of all driving cars
        
        # LAP DATA
        if packet_type == 2:
            newLap = False
            car_laptime_data = packet.lapData[position]
            if car_laptime_data.currentLapNum  > lastLap:
               newLap = True
               lastLap = car_laptime_data.currentLapNum
               sector3time = car_laptime_data.lastLapTime - (car_laptime_data.sector1TimeInMS + car_laptime_data.sector1TimeInMS)
               print(f"Starting Lap {lastLap}...")
            elif  car_laptime_data.sector1TimeInMS == 0 and car_laptime_data.sector1TimeInMS == 0:
                  sector3time = 0
            metrics.write_lap_data_to_splunk(driver_name, car_laptime_data, sector3time) 
         

        # TELEMETRY DATA
        if packet_type == 6:
            car_telemetry_data = packet.carTelemetryData[position]
            metrics.write_telemetry_data_to_splunk(driver_name, car_telemetry_data)
