import json
import os
import sys
import pandas as pd

pd.set_option('display.max_columns', None)

from schedule_data_processing.package import data


#  - Task 3
# When we try to look up for a flight which isn't in the data, we print a message "Value not matched"


def main(args):
    command = args[1]
    if command == "lookup":

        # Split the csv in the argument
        flight_numbers = args[2].split(",")

        # Print the flights
        print(flight_numbers)

        # Create a list to capture all the flights mentioned in the list
        flight = []

        # Loop through each flight and continue transformation
        for flight_number in flight_numbers:

            # Obtain data
            data.get_data()

            # Assign data to variables
            schedule = data.SCHEDULE
            fleet = data.FLEET
            airports = data.AIRPORTS

            # Required transformation
            fleet["aircraft_registration"] = fleet["Reg"]
            joined = schedule.merge(fleet, on="aircraft_registration")
            result = joined[joined.flight_number == flight_number]

            # If the length of the result is equal to 0 - we don't have an entry in the data, hence we throw error
            if (len(result) == 0):
                print("********************Value not matched*************************")
            else:
                # Convert result to key value pair
                result = result.to_dict(orient="list")
                # Append the result to list
                flight.append(result)
                # Create a list of key
                keys = []
                # Required transformation
                for y in range(0, len(flight)):
                    for x in flight[y].keys():
                        keys.append(x)
                    for x in keys:
                        if x in "F,C,E,M":
                            del flight[y][x]
                        elif x in "RangeLower,RangeUpper,Reg":
                            del flight[y][x]
                        elif x == "Total":
                            flight[y]["total_seats"] = str(flight[y][x][0])
                            del flight[y][x]
                        else:
                            pass
                            flight[y][x] = str(flight[y][x][0])
                # Print the o/p
                #print(flight)
        return flight


if __name__ == "__main__":
    print(main(sys.argv))
