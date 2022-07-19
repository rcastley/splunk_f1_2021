from telemetry import server

if __name__ == "__main__":
    driver_name = str(input("Driver name: ")).strip().lower()
    server.start_driver(driver_name)
