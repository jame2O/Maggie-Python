import firebase_admin
from firebase_admin import firestore, credentials
import pandas as pd
from datetime import datetime
import re
import googlemaps
import json
cred = credentials.Certificate("./python_scripts/prod-key.json")

app = firebase_admin.initialize_app(cred)
db = firestore.client()

GOOGLE_API_KEY = ""
with open("./python_scripts/google_api_key.json") as f:
    GOOGLE_API_KEY = json.load(f)["key"]
    print(GOOGLE_API_KEY)
    f.close()

gmaps = googlemaps.Client(key=GOOGLE_API_KEY)

def get_lat_lng(address, name):
    location = None
    # Try geocoding by address. If this doesn't work, try the name. If else, return nonetypes
    geocode = gmaps.geocode(address) 
    if geocode:
        location = gmaps.geocode(address)[0]["geometry"]["location"]
        return location["lat"], location["lng"]
    
    geocode = gmaps.geocode(name)
    if geocode:
        location = gmaps.geocode(address)[0]["geometry"]["location"]
        return location["lat"], location["lng"]
    #location = gmaps.geocode(address)[0]["geometry"]["location"]
    return None, None


def process_parks(df):
    parks = []
    for index, row in df.iterrows():
        doc_entry = {
            "state": "",
            "council": "",
            "location": {
                "name": "",
                "address": "",
                "latlng": None
            },
            "age_range": {
                "min": 0.0,
                "max": 0.0,
            },
            "name": "",
            "description": "",
            "icon": ""
        }
        doc_entry["state"] = row["State"].lower()
        doc_entry["council"] = row["Council"]
        doc_entry["location"]["name"] = row["Where?"]
        doc_entry["location"]["address"] = row["Address"]
        doc_entry["icon"] = row["Icon"]
        doc_entry["description"] = row["Short description"]
        doc_entry["name"] = row["What's On?"]
        
        # Process age
        min_age, max_age = get_age_ranges(row["Suitable for"])
        if min_age is not None:
            doc_entry["age_range"]["min"] = float(min_age.replace(" ", ""))
        if max_age is not None:    
            doc_entry["age_range"]["max"] = float(max_age.replace(" ", ""))
        # Process loc
        if doc_entry["location"]["address"] is not None:
            lat, lng = get_lat_lng(doc_entry["location"]["address"], doc_entry["location"]["name"])
            if lat is not None and lng is not None:
                doc_entry["location"]["latlng"] = firestore.GeoPoint(lat, lng)
                doc_entry["latitude"] = lat
                doc_entry["longitude"] = lng
                

        parks.append(doc_entry)
    return parks

def process_activities(df):
    activities = []
    for index, row in df.iterrows():
        # Define the entry struct
        doc_entry = {
            "state": "",
            "council": "",
            "location": {
                "name": "",
                "address": "",
                "latlng": None
            },
            "time": {
                "day": "",
                "time_range": {
                    "start": "",
                    "end": "",
                },
            },
            "age_range": {
                "min": 0.0,
                "max": 0.0,
            },
            "name": "",
            "description": "",
            "icon": ""
        } 
        doc_entry["state"] = row["State"].lower()
        doc_entry["council"] = row["Council"]
        doc_entry["location"]["name"] = row["Where?"]
        doc_entry["location"]["address"] = row["Address"]
        doc_entry["icon"] = row["Icon"]
        doc_entry["name"] = row["What's On?"]
        doc_entry["time"]["day"] = row["Day"]
        doc_entry["description"] = row["Short description"]
        # Process time
        min_time, max_time = get_time_ranges(row["Time"])
        if min_time is not None:
            # Convert to 24 hour format
            time_tw = min_time.replace(" ", "").replace(".", ":")
            time_tf = datetime.strptime(time_tw, '%I:%M%p')
            time_tfS = time_tf.strftime('%H:%M')
            doc_entry["time"]["time_range"]["start"] = time_tfS
        if max_time is not None:
            time_tw = max_time.replace(" ", "").replace(".", ":")
            time_tf = datetime.strptime(time_tw, '%I:%M%p')
            time_tfS = time_tf.strftime('%H:%M')
            doc_entry["time"]["time_range"]["end"] = time_tfS
        # Process age
        min_age, max_age = get_age_ranges(row["Suitable for"])
        if min_age is not None:
            doc_entry["age_range"]["min"] = float(min_age.replace(" ", ""))
        if max_age is not None:    
            doc_entry["age_range"]["max"] = float(max_age.replace(" ", ""))
        # Process loc
        if doc_entry["location"]["address"] is not None:
            lat, lng = get_lat_lng(doc_entry["location"]["address"], doc_entry["location"]["name"])
            if lat is not None and lng is not None:
                doc_entry["location"]["latlng"] = firestore.GeoPoint(lat, lng)
                doc_entry["latitude"] = lat
                doc_entry["longitude"] = lng
                
        activities.append(doc_entry)    
    return activities
def update_data(states, data, org, rep):
    for state in states:
        coll_ref = db.collection("activity_data", "activities", f"{state}")
        query = coll_ref.where(data, "==", org)
        docs = query.stream()
        print("hello")
        for doc in docs:
            doc_ref = coll_ref.document(doc.id)
            doc_ref.update({data: rep})
            print(f'Document {doc.id} updated: {data} changed from {org} to {rep}')
    
def upload_data(data, type):
    for doc_entry in data:
        state = doc_entry["state"]
        coll_ref = db.collection("activity_data", f"{type}", f"{state}")
        # We don't need to store state data in each entry.
        del doc_entry["state"]
        doc_ref = coll_ref.add(doc_entry)
        print(f'Document created with ID: {doc_ref[1].id}')
        

def get_time_ranges(text):
    pattern = r"([\d]{1,2}(.[\d]{1,2})?\s?[a-zA-Z]{2})\s*(-\s*([\d]{1,2}(.[\d]{1,2})?\s?[a-zA-Z]{2}))?"
    match = re.search(pattern, text)
    if (match):
        return match.group(1), match.group(4)
    else:
        return None, None
def get_age_ranges(text):
    pattern = r"([\d]{1,2}(\.\d)?)\s*-\s*([\d]{1,2}(\.\d)?)"
    match = re.search(pattern, text)
    if (match):
        return match.group(1), match.group(3)
    else:
        return None, None
def extract_times_to_csv(input, output):
    df = pd.read_csv(input)
    output_rows = []
    time_pattern = r"([\d]{1,2}(:[\d]{2})?\s*[apAP][mM])\s*[-–—]\s*([\d]{1,2}(:[\d]{2})?\s*[apAP][mM])"
    for idx, row in df.iterrows():
        name=row["What's On?"]
        location=row["Where?"]
        time_str= str(row.get("Time", ""))
        day=row["Day"]
        time_str = time_str.split('\n')[0]

        match = re.search(time_pattern, time_str)
        if match:
            start_raw = match.group(1).replace(" ", "")
            end_raw = match.group(3).replace(" ", "")
            try:
                start_24 = datetime.strptime(start_raw, "%I:%M%p").strftime("%H:%M")
            except ValueError:
                start_24 = datetime.strptime(start_raw, "%I%p").strftime("%H:%M")
            try:
                end_24 = datetime.strptime(end_raw, "%I:%M%p").strftime("%H:%M")
            except ValueError:
                end_24 = datetime.strptime(end_raw, "%I%p").strftime("%H:%M")
            output_rows.append({"name": name, "start_time": start_24, "end_time": end_24, "day": day, "location": location})
        else:
            print(f"No match for row {idx}: {name} | {day} | {location} | {time_str}")

    out_df = pd.DataFrame(output_rows)
    out_df.to_csv(output, index=False)
    print(f"Saved to {output}")
    

def update_activity_times(state, csv_path):
    """
    Update Firestore activity times for a given state using a CSV with columns:
    name, start_time, end_time, day, location.
    Only updates entries where time.start or time.end is an empty string,
    and matches on name, day, and location.
    """
    df = pd.read_csv(csv_path)
    coll_ref = db.collection("activity_data").document("activities").collection(state)
    docs = coll_ref.stream()
    for doc in docs:
        data = doc.to_dict()
        # Defensive: check structure
        if "time" in data and "time_range" in data["time"]:
            start_empty = data["time"]["time_range"].get("start", "") == ""
            end_empty = data["time"]["time_range"].get("end", "") == ""
            if start_empty or end_empty:
                # Match on name, day, and location
                name = data.get("name", "")
                day = data["time"].get("day", "")
                location = data.get("location", {}).get("name", "")
                row = df[
                    (df["name"] == name) &
                    (df["day"] == day) &
                    (df["location"] == location)
                ]
                if not row.empty:
                    start_time = row.iloc[0]["start_time"]
                    end_time = row.iloc[0]["end_time"]
                    doc.reference.update({
                        "time.time_range.start": start_time,
                        "time.time_range.end": end_time
                    })
                    print(f"Updated {name} ({day}, {location}) with start: {start_time}, end: {end_time}")
                else:
                    print(f"No CSV match for {name} ({day}, {location})")
if __name__ == '__main__':
    states = ["NSW","VIC","WA","SA","TAS","ACT","QLD"]
    for state in states:
        try:
            update_activity_times(state.lower(), f"./python_scripts/data/{state}ExtraActivityTimes.csv")
        except FileNotFoundError:
            print(f"No file found for {state}")
            continue


    