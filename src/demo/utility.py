import random
import os

sensors = [
    ("attic", {"temp": 104, "scale": "F", "humidity": 28}),
    ("basement", {"temp": 61, "scale": "F", "humidity": 72}),
    ("basement-dehumidifier", {"temp": 63, "scale": "F", "humidity": 71}),
    ("bathroom-main", {"temp": 70, "scale": "F", "humidity": 80}),
    ("bathroom-master", {"temp": 71, "scale": "F", "humidity": 78}),
    ("bedroom-2", {"temp": 69, "scale": "F", "humidity": 45}),
    ("bedroom-3", {"temp": 68, "scale": "F", "humidity": 44}),
    ("dining-room", {"temp": 71, "scale": "F", "humidity": 47}),
    ("garage", {"temp": 55, "scale": "F", "humidity": 60}),
    ("garage-freezer", {"temp": 0, "scale": "F", "humidity": 30}),
    ("greenhouse", {"temp": 27, "scale": "C", "humidity": 85}),
    ("guest-room", {"temp": 68, "scale": "F", "humidity": 46}),
    ("home-office", {"temp": 72, "scale": "F", "humidity": 43}),
    ("hvac-zone-1", {"temp": 71, "scale": "F", "humidity": 48}),
    ("hvac-zone-2", {"temp": 70, "scale": "F", "humidity": 47}),
    ("kitchen", {"temp": 74, "scale": "F", "humidity": 52}),
    ("kitchen-freezer", {"temp": -18, "scale": "C", "humidity": 25}),
    ("kitchen-refrigerator", {"temp": 37, "scale": "F", "humidity": 55}),
    ("laundry-room", {"temp": 73, "scale": "F", "humidity": 65}),
    ("living-room", {"temp": 72, "scale": "F", "humidity": 46}),
    ("master-bedroom", {"temp": 68, "scale": "F", "humidity": 44}),
    ("mudroom", {"temp": 65, "scale": "F", "humidity": 58}),
    ("nursery", {"temp": 70, "scale": "F", "humidity": 50}),
    ("outdoor-back", {"temp": 82, "scale": "F", "humidity": 63}),
    ("outdoor-front", {"temp": 81, "scale": "F", "humidity": 62}),
    ("outdoor-side", {"temp": 82, "scale": "F", "humidity": 63}),
    ("pantry", {"temp": 65, "scale": "F", "humidity": 40}),
    ("pool-house", {"temp": 85, "scale": "F", "humidity": 75}),
    ("server-room", {"temp": 18, "scale": "C", "humidity": 35}),
    ("sunroom", {"temp": 78, "scale": "F", "humidity": 55}),
    ("utility-closet", {"temp": 68, "scale": "F", "humidity": 48}),
    ("wine-cellar", {"temp": 13, "scale": "C", "humidity": 70}),
    ("wine-cellar-cooler", {"temp": 12, "scale": "C", "humidity": 72}),
    ("boiler-room", {"temp": 85, "scale": "F", "humidity": 55}),
    ("crawl-space", {"temp": 58, "scale": "F", "humidity": 78}),
    ("deck-north", {"temp": 80, "scale": "F", "humidity": 61}),
    ("deck-south", {"temp": 83, "scale": "F", "humidity": 60}),
    ("den", {"temp": 71, "scale": "F", "humidity": 45}),
    ("dog-kennel", {"temp": 68, "scale": "F", "humidity": 50}),
    ("driveway", {"temp": 79, "scale": "F", "humidity": 58}),
    ("entryway", {"temp": 70, "scale": "F", "humidity": 47}),
    ("exercise-room", {"temp": 73, "scale": "F", "humidity": 52}),
    ("foyer", {"temp": 70, "scale": "F", "humidity": 46}),
    ("game-room", {"temp": 71, "scale": "F", "humidity": 44}),
    ("hallway-upper", {"temp": 69, "scale": "F", "humidity": 45}),
    ("hallway-lower", {"temp": 70, "scale": "F", "humidity": 46}),
    ("media-room", {"temp": 70, "scale": "F", "humidity": 43}),
    ("outdoor-roof", {"temp": 95, "scale": "F", "humidity": 55}),
    ("sauna", {"temp": 80, "scale": "C", "humidity": 20}),
    ("shed", {"temp": 77, "scale": "F", "humidity": 62}),
    ("storage-room", {"temp": 66, "scale": "F", "humidity": 50}),
    ("water-heater-closet", {"temp": 90, "scale": "F", "humidity": 40}),
]


def random_customers(raw: str):
    cust_count = try_to_int(raw) or 1

    result = []
    for _ in range(0, cust_count):
        idx = str(random.randint(0, 9999999)).zfill(7)
        result.append(idx)
    return result


def data_root_path():
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def delete_data_files(parent_directory):
    extension = ".jsonl"

    for dirname, _, files in os.walk(parent_directory):
        for file in files:
            if file.lower().endswith(extension.lower()):
                file_path = os.path.join(dirname, file)
                try:
                    os.remove(file_path)
                    print(f"Deleted: {file_path}")
                except OSError as e:
                    print(f"Error deleting {file_path}: {e}")


def random_sensor_data():
    idx = random.randint(0, len(sensors) - 1)
    sensor = sensors[idx]
    result = sensor[0]

    result += f",{sensor[1]['temp']}{sensor[1]['scale']},{sensor[1]['humidity']}"
    return result


def try_to_int(value: str):
    try:
        return int(value)
    except Exception:
        return None
