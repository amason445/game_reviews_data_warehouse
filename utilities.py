import json

#function that converts dictionary/list to JSON and writes it to a JSON document file for reference/debugging
def write_to_json(dictionary: dict, file_path: str) -> None:
    json_output = json.dumps(dictionary)
    with open(file_path, 'w') as f:
        f.write(json_output)
        f.close()

#function that allows you read a JSON document file and write it to a dictionary/list
def read_from_json(file_path: str):
    with open(file_path, 'r') as f:
        dictionary = json.load(f)
        f.close
    return dictionary