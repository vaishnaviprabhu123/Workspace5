import json
import os
import sys

import mpu
import pandas as pd
from math import sin, cos, sqrt, atan2, radians
from geopy.distance import *

# Task 1 - To include nautical miles
# Wasn't able to look for the particular library, however I have included the skeleton code of what I have tried

pd.set_option('display.max_columns', None)

from schedule_data_processing.package import data


def main(args):
    command = "merge"
    if command == "merge":
        data.get_schedule()
        data.get_airports()
        data.get_fleet()
        schedule = data.SCHEDULE
        fleet = data.FLEET
        airports = data.AIRPORTS
        fleet["aircraft_registration"] = fleet["Reg"]
        # Join schedule & fleet data
        joined = schedule.merge(fleet, on="aircraft_registration")
        # Create new column by assignment
        airports["departure_airport"] = airports["Airport"]
        # Join previous df with airport data
        joined = joined.merge(airports, on="departure_airport", suffixes=(None, "_departure"))
        # Create new column by assignment
        airports["arrival_airport"] = airports["Airport"]
        # Join previous joined df with airport df
        joined = joined.merge(airports, on="arrival_airport", suffixes=(None, "_arrival"))
        # Drop unnecessary col and save as csv file
        joined.drop(columns=["departure_airport_arrival"], inplace=True)
        # print(joined)

        '''joined["lat_diff"] = joined["Lat"].astype(int) - joined["Lat_arrival"].astype(int)
        joined["long_diff"] = joined["Lon"].x - joined["Lon_arrival"].astype(int)
        joined["formula1"] = sin(joined["lat_diff"] / 2) ** 2 + cos(joined["Lat"]) * cos(joined["Lat_arrival"]) * sin(joined["long_diff"] / 2) ** 2
        joined["formula2"] = 2 * atan2(sqrt(joined["formula1"], sqrt(1 - joined["formula1"])))

        #a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        #c = 2 * atan2(sqrt(a), sqrt(1 - a))
        #Approx radius of earth
        joined["distance"] = 6373.0 * joined["formula2"]'''
        # distance = R * c
        # print(joined)


if __name__ == "__main__":
    print(main(sys.argv))
