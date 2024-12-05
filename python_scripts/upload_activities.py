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

def get_lat_lng(address):
    location = gmaps.geocode(address)[0]["geometry"]["location"]
    return location["lat"], location["lng"]

def upload_activities(df):
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
            doc_entry["time"]["time_range"]["start"] = min_time.replace(" ", "")
        if max_time is not None: 
            doc_entry["time"]["time_range"]["end"] = max_time.replace(" ", "") 
        # Process age
        min_age, max_age = get_age_ranges(row["Suitable for"])
        if min_age is not None:
            doc_entry["age_range"]["min"] = min_age.replace(" ", "")
        if max_age is not None:    
            doc_entry["age_range"]["max"] = max_age.replace(" ", "")
        # Process loc
        if doc_entry["location"]["address"] is not None:
            lat, lng = get_lat_lng(doc_entry["location"]["address"])
            if lat is not None and lng is not None:
                doc_entry["location"]["latlng"] = firestore.GeoPoint(lat, lng)
            
        print(doc_entry)
        coll_ref = db.collection("activity_data", "activities", f"{doc_entry["state"]}")
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
    upload_activities(vic_master_df)