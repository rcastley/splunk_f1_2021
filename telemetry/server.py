import socket
import yaml
import telemetry.splunk as metrics
from telemetry_f1_2021.listener import TelemetryListener
from socket import gethostname, getfqdn, gethostbyname

with open("config.yaml", "r") as ymlfile:
    cfg = yaml.safe_load(ymlfile)


def start_driver(driver_name):
    listener = TelemetryListener(port=cfg["UDP_PORT"])
    print(f"Session for driver {driver_name} started ...")
    print(
        f"UDP Server listening to port {cfg['UDP_PORT']} on IP address: {socket.gethostbyname(socket.getfqdn())}"
    )
    showGameInfo = True
    # Receive Packages
    while True:
        packet = listener.get()
        if showGameInfo== True:
            print(f"Game version: {packet.m_header.m_game_major_version}.{packet.m_header.m_game_minor_version} Packet format: {packet.m_header.m_packet_format}") 
            showGameInfo = False
        packet_type = packet.m_header.m_packet_id  # get the packet type from the header
        position = packet.m_header.m_player_car_index  # get the position of the driver
        session_uid = packet.m_header.m_session_uid  # get the session uid  

        # SESSION DATA
        if packet_type == 1:
            if position is not None:
                metrics.set_dimensions(session_uid, driver_name, packet.m_track_id)
            metrics.write_temperatures(packet.m_track_temperature, packet.m_air_temperature)

        # LAP DATA
        if packet_type == 2:
            metrics.write_lap_data(packet.m_lap_data[position])
 
        # TELEMETRY DATA
        if packet_type == 6:
            metrics.write_telemetry_data(packet.m_car_telemetry_data[position])
            