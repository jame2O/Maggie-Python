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
            time_tw = min_time.replace(" ", "")
            time_tf = datetime.strptime(time_tw, '%I:%M%p')
            time_tfS = time_tf.strftime('%H:%M')
            doc_entry["time"]["time_range"]["start"] = time_tfS
        if max_time is not None: 
            time_tw = min_time.replace(" ", "")
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
        activities.append(doc_entry)    
    return activities

def upload_data(data, type):
    for doc_entry in data:
        print(doc_entry)
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

if __name__ == '__main__':
    # Load datasets
    vic_master_df = pd.read_csv("./python_scripts/data/vicMaster.csv")
    nsw_master_df = pd.read_csv("./python_scripts/data/nswMaster.csv")
    vic_parks_df = pd.read_csv("./python_scripts/data/vicParks.csv")
    
    parks = process_activities(vic_master_df)
    print(parks)
    upload_data(parks, "parks")