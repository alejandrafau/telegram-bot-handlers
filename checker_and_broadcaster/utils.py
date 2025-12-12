import json
import json


def read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
            return obj
    except Exception as e:
        return e


def write_json(path, obj):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        print(f"Error writing JSON: {e}")
        return False
