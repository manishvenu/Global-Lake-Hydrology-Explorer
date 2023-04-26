from GLHE import ERA5_Land



def main():
    print(ERA5_Land.request_temperature_data())
    print("Ran Main")

if __name__ == "__main__":
    main()