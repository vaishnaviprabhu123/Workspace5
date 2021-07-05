import json
import os
import sys
import pandas as pd

pd.set_option('display.max_columns', None)

from schedule_data_processing.package import data


# Task 2 - Look for multiple flights

def main(args):
    command = args[1]
    if command == "lookup":
        # Split the csv elements
        flight_numbers = args[2].split(",")
        # Print the flights
        print(flight_numbers)
        # Create a list to include all flight entry
        flight = []
        # Loop through all the flights in the argument
        for flight_number in flight_numbers:
            # Import all the data from blob
            data.get_data()
            schedule = data.SCHEDULE
            fleet = data.FLEET

            # As airport data isn't used - we can drop below line
            # airports = data.AIRPORTS

            # Required transformation
            fleet["aircraft_registration"] = fleet["Reg"]
            joined = schedule.merge(fleet, on="aircraft_registration")
            result = joined[joined.flight_number == flight_number]
            result = result.to_dict(orient="list")

            # Include a new list for each column entry
            keys = []
            # Required transformation
            for x in result.keys():
                keys.append(x)
            for x in keys:
                if x in "F,C,E,M":
                    del result[x]
                elif x in "RangeLower,RangeUpper,Reg":
                    del result[x]
                elif x == "Total":
                    result["total_seats"] = str(result[x][0])
                    del result[x]
                else:
                    result[x] = str(result[x][0])

            # Add each flight entry to list and return list of all the flights mentioned in argument
            flight.append(result)
        # print(flight)
        # Check for len of flight to cross verify the length of list to number of flights included
        print(len(flight))
        return json.dumps(flight)


if __name__ == "__main__":
    print(main(sys.argv))
