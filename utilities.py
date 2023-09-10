import json

#function that converts dictionary/list to JSON and writes it to a file for reference/debugging
def write_to_json(dictionary, file_path):
    json_output = json.dumps(dictionary)
    with open(file_path, 'w') as f:
        f.write(json_output)
        f.close()
