import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import json
import uuid

# --- CONFIGURATION ---
NUM_DAYS = 90  # <<<--- Increase this number to generate more data (e.g., 365 for a full year)
START_DATE = datetime(2025, 1, 1)
HOSPITAL_HOSTNAME = "www.apollohospitals.com"
PRIMARY_CITY = "Chennai"
PRIMARY_REGION = "Tamil Nadu"
PRIMARY_COUNTRY_CODE = "IN"
PRIMARY_COUNTRY_NAME = "India"

fake = Faker('en_IN') # Use Indian locale for more realistic data

print(f"Generating {NUM_DAYS} days of data...")

# --- DATA DEFINITIONS ---
# Define common search queries and their typical landing pages
GSC_QUERIES = {
    "knee pain": "/blog/knee-pain-causes-and-treatments/",
    "best orthopedic surgeon in chennai": "/chennai/centres-of-excellence/orthopedics/",
    "hip replacement options": "/chennai/centres-of-excellence/orthopedics/hip-replacement",
    "sports injury clinic chennai": "/chennai/centres-of-excellence/orthopedics/sports-medicine",
    "shoulder surgery recovery time": "/blog/shoulder-surgery-recovery-guide/",
    "apollo chennai appointment": "/chennai/patients/appointments",
    "best cardiologist in chennai": "/chennai/centres-of-excellence/cardiology/",
    "heart attack symptoms": "/blog/understanding-heart-attack-symptoms/",
    "maternity packages chennai": "/chennai/centres-of-excellence/maternity/packages"
}

# Define YouTube videos
YT_VIDEOS = {
    "yt_apollo_ortho_team_01": {"title": "Meet Our Orthopedic Team - Apollo Chennai", "duration_sec": 180, "avg_view_sec": 30},
    "yt_apollo_cardio_proc_02": {"title": "What to Expect During an Angioplasty", "duration_sec": 240, "avg_view_sec": 90},
    "yt_apollo_maternity_tour_03": {"title": "Virtual Tour of Apollo Maternity Ward Chennai", "duration_sec": 210, "avg_view_sec": 120}
}

# Define common page paths and titles
PAGE_MAP = {
    "/": "Homepage | Apollo Hospitals",
    "/chennai/centres-of-excellence/orthopedics/": "Best Orthopedic Hospital in Chennai | Apollo",
    "/blog/knee-pain-causes-and-treatments/": "Knee Pain Causes & Treatments",
    "/doctors/chennai/dr-aravind-kumar": "Dr. Aravind Kumar - Orthopedic Surgeon",
    "/chennai/patients/appointments": "Book an Appointment | Apollo Chennai",
    "/chennai/centres-of-excellence/cardiology/": "Best Cardiology Hospital in Chennai | Apollo",
    "/blog/understanding-heart-attack-symptoms/": "Understanding Heart Attack Symptoms",
    "/chennai/centres-of-excellence/maternity/packages": "Maternity Packages at Apollo Chennai"
}

# --- 1. GSC PERFORMANCE DATA GENERATION ---
print("Generating GSC Performance data...")
gsc_data = []
for day in range(NUM_DAYS):
    current_date = START_DATE + timedelta(days=day)
    for query, page in GSC_QUERIES.items():
        for device in ["MOBILE", "DESKTOP"]:
            impressions = random.randint(50, 8000) if "best" not in query else random.randint(500, 10000)
            
            # Simulate low CTR for the specific query from the demo script
            if query == "shoulder surgery recovery time":
                ctr = np.random.normal(0.005, 0.001)
            else:
                ctr = np.random.normal(0.08, 0.03) # Normal CTR
            
            clicks = max(0, int(impressions * ctr))
            
            # Avg position is sum_position / impressions
            avg_position = np.random.normal(3.5, 1.5) if "best" in query else np.random.normal(8, 4)
            sum_position = int(avg_position * impressions)

            gsc_data.append({
                "partition_date": current_date.strftime('%Y-%m-%d'),
                "query": query,
                "page_url": f"https://{HOSPITAL_HOSTNAME}{page}",
                "country": PRIMARY_COUNTRY_CODE,
                "device": device,
                "clicks": clicks,
                "impressions": impressions,
                "sum_position": sum_position
            })

gsc_df = pd.DataFrame(gsc_data)
gsc_df.to_csv("mock_gsc_performance.csv", index=False)
print(f" -> Saved {len(gsc_df)} rows to mock_gsc_performance.csv")

# --- 2. YOUTUBE ANALYTICS DATA GENERATION ---
print("Generating YouTube Analytics data...")
yt_data = []
for day in range(NUM_DAYS):
    current_date = START_DATE + timedelta(days=day)
    for video_id, video_meta in YT_VIDEOS.items():
        views = random.randint(50, 1500)
        watch_time_msec = views * video_meta['avg_view_sec'] * 1000
        potential_watch_time_msec = views * video_meta['duration_sec'] * 1000
        
        yt_data.append({
            "partition_date": current_date.strftime('%Y-%m-%d'),
            "external_video_id": video_id,
            "video_title": video_meta['title'],
            "country_code": PRIMARY_COUNTRY_CODE,
            "age_group": random.choice(['25-34', '35-44', '45-54', '55-64']),
            "gender": random.choice(['MALE', 'FEMALE']),
            "device_type": random.choice(['MOBILE', 'DESKTOP', 'TABLET']),
            "traffic_source_type": random.choice(['YT_SEARCH', 'YT_RELATED', 'SUBSCRIBER', 'EXT_URL']),
            "views": views,
            "watch_time_msec": watch_time_msec,
            "potential_watch_time_msec": potential_watch_time_msec,
            "likes_added": int(views * np.random.normal(0.02, 0.005)),
            "shares": int(views * np.random.normal(0.005, 0.002)),
            "comments_added": int(views * np.random.normal(0.002, 0.001)),
            "subscribers_gained": int(views * np.random.normal(0.001, 0.0005))
        })

yt_df = pd.DataFrame(yt_data)
yt_df.to_csv("mock_youtube_analytics.csv", index=False)
print(f" -> Saved {len(yt_df)} rows to mock_youtube_analytics.csv")

# --- 3. GA SESSIONS DATA GENERATION ---
print("Generating GA Sessions data...")
ga_sessions_data = []
for day in range(NUM_DAYS):
    current_date = START_DATE + timedelta(days=day)
    num_sessions_today = random.randint(200, 1000) # Number of sessions per day
    
    for _ in range(num_sessions_today):
        session_start_time = current_date + timedelta(seconds=random.randint(0, 86399))
        user_id = fake.uuid4()
        session_id = str(uuid.uuid4())
        
        # Traffic Source Logic
        traffic_source_roll = random.random()
        if traffic_source_roll < 0.6: # 60% Organic
            source, medium = 'google', 'organic'
        elif traffic_source_roll < 0.8: # 20% Direct
            source, medium = '(direct)', '(none)'
        elif traffic_source_roll < 0.9: # 10% Referral
            source, medium = random.choice(['youtube.com', 'facebook.com']), 'referral'
        else: # 10% CPC
            source, medium = 'google', 'cpc'

        # User Journey Simulation
        hits = []
        page_path = random.choice(list(PAGE_MAP.keys()))
        page_title = PAGE_MAP[page_path]
        
        # First hit is always a page view
        hits.append({
            "hit_number": 1, "hit_time": session_start_time.isoformat(), "type": "PAGE",
            "page": {"page_path": page_path, "page_title": page_title, "hostname": HOSPITAL_HOSTNAME}, "event_info": None
        })
        
        # Decide if the session will have more hits (not bounce)
        is_bounce = random.random() < 0.65 # 65% bounce rate, as per demo script
        time_on_site = 0
        
        if not is_bounce:
            num_additional_hits = random.randint(1, 4)
            last_hit_time = session_start_time
            for i in range(num_additional_hits):
                time_delta = timedelta(seconds=random.randint(30, 120))
                hit_time = last_hit_time + time_delta
                
                # Add another page view
                page_path = random.choice(list(PAGE_MAP.keys()))
                page_title = PAGE_MAP[page_path]
                hits.append({
                    "hit_number": i + 2, "hit_time": hit_time.isoformat(), "type": "PAGE",
                    "page": {"page_path": page_path, "page_title": page_title, "hostname": HOSPITAL_HOSTNAME}, "event_info": None
                })
                last_hit_time = hit_time
            
            # Maybe add a conversion event
            if random.random() < 0.2: # 20% of engaged sessions convert
                time_delta = timedelta(seconds=random.randint(10, 30))
                hit_time = last_hit_time + time_delta
                hits.append({
                    "hit_number": len(hits) + 1, "hit_time": hit_time.isoformat(), "type": "EVENT", "page": None,
                    "event_info": {"event_category": "Conversion", "event_action": "Request Appointment", "event_label": "Ortho Page"}
                })
                last_hit_time = hit_time

            time_on_site = int((last_hit_time - session_start_time).total_seconds())

        else: # It's a bounce
            time_on_site = random.randint(5, 45)

        ga_sessions_data.append({
            "partition_date": current_date.strftime('%Y-%m-%d'),
            "session_id": session_id,
            "user_id": user_id,
            "session_start_time": session_start_time.isoformat(),
            "device_category": random.choice(['mobile', 'desktop', 'tablet']),
            "browser": random.choice(['Chrome', 'Safari', 'Firefox', 'Edge']),
            "operating_system": random.choice(['Android', 'iOS', 'Windows', 'MacOS']),
            "geo": json.dumps({"country": PRIMARY_COUNTRY_NAME, "region": PRIMARY_REGION, "city": random.choice([PRIMARY_CITY, "Coimbatore", "Madurai"])}),
            "traffic_source": json.dumps({"source": source, "medium": medium, "campaign": "(not set)"}),
            "totals": json.dumps({"pageviews": len([h for h in hits if h['type'] == 'PAGE']), "time_on_site_seconds": time_on_site, "bounces": 1 if is_bounce else 0}),
            "hits": json.dumps(hits)
        })

ga_df = pd.DataFrame(ga_sessions_data)
# Convert complex columns to JSON strings as required by BigQuery CSV loader
ga_df.to_csv("mock_ga_sessions.csv", index=False)
print(f" -> Saved {len(ga_df)} rows to mock_ga_sessions.csv")
print("\nGeneration complete!")