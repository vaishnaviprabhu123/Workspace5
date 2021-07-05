import io
import os

import pandas as pd
import os

from azure.storage.blob import BlockBlobService

SCHEDULE = None
FLEET = None
AIRPORTS = None

# Included Task 4 for writing to log file when we obtain response from blob request

def cleanup_temp_files(schedule_file, fleet_file, airports_file):
    for x in [schedule_file, fleet_file, airports_file]:
        if os.path.exists(x):
            os.remove(x)


def get_schedule():
    global SCHEDULE
    # Connect to the block blob
    block_blob_service = BlockBlobService(account_name='zerogrecruiting',
                                          account_key='q9HNK+vY0InVSBmwM45KcOL7BZJJyBMWDwTNdwKPuqS83Iq8RP4lWETgCUKQkOOsJg4WjAsgdb21Dl8JpU6vkQ==')
    # Writing the response to the log file
    file = open("log.txt", "w")
    file.write("\nResponse: " + str(block_blob_service))

    # Include Blob name & file name
    blob_item = block_blob_service.get_blob_to_bytes('python-case-study', 'schedule.json')
    file.write("\nResponse: " + str(blob_item))
    file.close()

    # Read the json file
    SCHEDULE = pd.read_json(blob_item.content)
    cleanup_temp_files("schedule.json", "fleet.csv", "airports.csv")
    return SCHEDULE


def get_fleet():
    global FLEET
    # Connect to the block blob
    block_blob_service = BlockBlobService(account_name='zerogrecruiting',
                                          account_key='q9HNK+vY0InVSBmwM45KcOL7BZJJyBMWDwTNdwKPuqS83Iq8RP4lWETgCUKQkOOsJg4WjAsgdb21Dl8JpU6vkQ==')
    # Writing the response to the log file
    file = open("log.txt", "a")
    file.write("\nResponse: " + str(block_blob_service))
    # Include Blob name & file name
    blob_item = block_blob_service.get_blob_to_path('python-case-study', 'fleet.csv', 'fleet.csv')
    file.write("\nResponse: " + str(blob_item))
    # Close the File
    file.close()
    # Read the csv file
    FLEET = pd.read_csv('fleet.csv')
    cleanup_temp_files("schedule.json", "fleet.csv", "airports.csv")
    return FLEET


def get_airports():
    global AIRPORTS
    # Connect to the block blob
    block_blob_service = BlockBlobService(account_name='zerogrecruiting',
                                          account_key='q9HNK+vY0InVSBmwM45KcOL7BZJJyBMWDwTNdwKPuqS83Iq8RP4lWETgCUKQkOOsJg4WjAsgdb21Dl8JpU6vkQ==')
    # Writing the response to the log file
    file = open("log.txt", "a")
    file.write("\nResponse: " + str(block_blob_service))
    # Include Blob name & file name
    blob_item = block_blob_service.get_blob_to_path('python-case-study', 'airports.csv', 'airports.csv')
    file.write("\nResponse: " + str(blob_item))
    file.close()
    # Read the csv file
    AIRPORTS = pd.read_csv('airports.csv')
    cleanup_temp_files("schedule.json", "fleet.csv", "airports.csv")
    return AIRPORTS


def get_data():
    get_schedule()
    get_airports()
    get_fleet()

