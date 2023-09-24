import json
import time

#function that converts dictionary/list to JSON and writes it to a JSON document file for reference/debugging
def write_to_json(dictionary: dict, file_path: str) -> None:
    json_output = json.dumps(dictionary)
    with open(file_path, 'w', encoding= 'utf-8') as f:
        f.write(json_output)
        f.close()

#function that allows you read a JSON document file and write it to a dictionary/list
def read_from_json(file_path: str):
    with open(file_path, 'r', encoding= 'utf-8') as f:
        dictionary = json.load(f)
        f.close
    return dictionary

#function to pause script if request threshhold is breached to prevent API throttling
def request_break(current_requests, request_pause_threshhold = 100):
    if current_requests > request_pause_threshhold:
        time.sleep(3)