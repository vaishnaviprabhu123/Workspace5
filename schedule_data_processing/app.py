import sys
import json
import pandas as pd
pd.set_option('display.max_columns', None)

if __name__ == "__main__":
    from package import data
else:
    from schedule_data_processing.package import data

def main(args):

    command = args[1]

    if command == "lookup":
        flight_numbers = args[2].split(",")
        print(flight_numbers)
        for flight_number in flight_numbers:
            data.get_data()
            schedule = data.SCHEDULE
            fleet = data.FLEET
            airports = data.AIRPORTS
            fleet["aircraft_registration"] = fleet["Reg"]
            joined = schedule.merge(fleet, on="aircraft_registration")
            result = joined[joined.flight_number == flight_number]
            result = result.to_dict(orient="list")
            keys = []
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
            print(result)
            return json.dumps(result)

    if command == "merge":
        data.get_schedule()
        data.get_airports()
        data.get_fleet()
        schedule = data.SCHEDULE
        fleet = data.FLEET
        airports = data.AIRPORTS
        print(schedule)
        print(fleet)
        print(airports)
        # Create new column by assignment
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
        joined.to_csv("output.csv")
        return ""



if __name__ == "__main__":
   print(main(sys.argv))
