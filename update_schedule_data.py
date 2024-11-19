import os
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import pytz
from isodate import parse_duration
from googleapiclient.discovery import build
from dotenv import load_dotenv

def sanitize_text(text: str) -> str:
    """Replace newline characters with HTML break tags."""
    return text.replace("\n", "<br>") if text else None

def format_duration(duration):
    days, seconds = divmod(duration.total_seconds(), 86400)
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if days:
        hours += int(days) * 24

    return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

def check_stream_state_type(item):
    ARCHIVED = "archived"
    VIDEO = "video"
    LIVE = "live"

    ON_ENDED = "on ended"
    ON_LIVE = "on live"
    ON_SCHEDULED = "on scheduled"

    live_content_state_mapping = {
        "none": ON_ENDED,
        "live": ON_LIVE,
        "upcoming": ON_SCHEDULED,
    }

    live_content = item['snippet']['liveBroadcastContent']  # ("none" = on ended, "live" = on live, "upcoming" = on scheduled)
    upload_status = item['status']['uploadStatus'] # ("processed" = on archived / on scheduled video, "uploaded" = on live / on scheduled live)

    stream_state = live_content_state_mapping.get(live_content, "UNKNOWN")

    if stream_state == ON_ENDED:
        if "liveStreamingDetails" in item:
            stream_type = VIDEO
        else:
            stream_type = LIVE
    elif upload_status == "processed":
        stream_type = VIDEO
    elif upload_status == "uploaded":
        stream_type = LIVE

    return stream_state, stream_type

def fetch_youtube_data(video_ids):
    """Fetch video data for a list of video video_ids."""
    items = []
    max_ids_per_request = 50
    youtube = build('youtube', 'v3',
                    developerKey = os.environ.get("YOUTUBE_API"))
    
    for i in range(0, len(video_ids), max_ids_per_request):
        chunk = video_ids[i:i + max_ids_per_request]
        try:
            response = youtube.videos().list(
                id=",".join(chunk),
                part="snippet,status,contentDetails,liveStreamingDetails"
            ).execute()
            items.extend(response.get('items', []))
        except Exception as e:
            print(f"Error fetching data for video_ids {chunk}: {e}")

    return items

def add_stream_time(date: datetime, stream_time: str):
    hour, minute = map(int, stream_time.strip().split(':'))

    return date.replace(hour=hour, minute=minute)

def localize_stream_date(stream_date: str, timezone: str):
    tz = pytz.timezone(timezone)
    month, day = map(int, stream_date.strip().split(' ')[0].split('/'))

    year = datetime.now(tz).year
    localized_date = tz.localize(datetime(year, month, day))

    return localized_date

def is_youtube_id(video_id):
    return isinstance(video_id, str) and len(video_id) == 11 and re.match(r'^[a-zA-Z0-9_-]+$', video_id)

def extract_id(url: str):
    if "abema" in url:
        return url.split('/')[-1]

    pattern = r'(?:https?://)?(?:www\.)?(?:youtube\.com/(?:[^/]+/.*|(?:v|e(?:mbed)?|watch\?v=)|.*[?&]v=)|youtu\.be/)([^&]{11})'

    match = re.search(pattern, url)
    if match:
        return match.group(1)  # Return the video ID

    return None  # Return None if no match is found

def scrape_website(url: str, timezone: str):
    return requests.get(url, cookies={'timezone': timezone})

def process_data(response: requests.Response, timezone: str):
    soup = BeautifulSoup(response.text, "html.parser")

    data = {}

    last_stream_date = (datetime.now(pytz.timezone(timezone)) - timedelta(days=1)).date()

    containers = soup.select("div#all > div.container")
    for container in containers:
        items = container.select("div.row > div")

        date_item = items[0].select_one("div.navbar-text")
        if date_item:
            last_stream_date = localize_stream_date(date_item.text, timezone)
            
        stream_items = items[1].select("div.row > div:nth-child(2) > div.row > div")
        for stream_item in stream_items:
            stream_link = stream_item.select_one("a.thumbnail").get("href")
            stream_id = extract_id(stream_link)

            _time = stream_item.select_one("div.datetime").text.strip()
            datetime_scheduled_start = add_stream_time(last_stream_date, _time).isoformat()
            channel_short_name = stream_item.select_one("div.name").text.strip()

            imgs = stream_item.select("img")

            stream_thumbnail = imgs[1].get("src")

            channel_collabs = []
            for image in imgs[2:]:
                channel_collabs.append(image.get("src"))

            data[stream_id] = {
                "stream": {
                    "link": stream_link,
                    "thumbnail": stream_thumbnail,
                },
                "datetime": {
                    "scheduled_start": datetime_scheduled_start,
                },
                "channel": {
                    "short_name": channel_short_name,
                    "collabs": channel_collabs
                }
            }

    video_ids = list(data.keys())

    youtube_video_ids = [vid for vid in video_ids if is_youtube_id(vid)]

    youtube_items: list[dict] = fetch_youtube_data(youtube_video_ids)

    for item in youtube_items:
        stream_id = item["id"]
        if stream_id in data:
            stream_status, stream_type = check_stream_state_type(item)
            
            duration = item.get("contentDetails", {}).get("duration")
            if duration is not None:
                duration = format_duration(parse_duration(item.get("contentDetails", {}).get("duration")))

            # Ensure the nested dictionaries exist before updating
            data[stream_id].setdefault("stream", {}).update({
                "title": item.get("snippet", {}).get("title"),
                "description": sanitize_text(item.get("snippet", {}).get("description")),
                "duration": duration,
                "status": stream_status,
                "type": stream_type,
            })

            # If scheduled_start is None, retain the original value
            scheduled_start = item.get("liveStreamingDetails", {}).get("scheduledStartTime")
            if scheduled_start is None:
                scheduled_start = data[stream_id]["datetime"]["scheduled_start"]

            data[stream_id].setdefault("datetime", {}).update({
                "scheduled_start": scheduled_start,
                "actual_start": item.get("liveStreamingDetails", {}).get("actualStartTime"),
                "actual_end": item.get("liveStreamingDetails", {}).get("actualEndTime"),
            })

            data[stream_id].setdefault("channel", {}).update({
                "name": item.get("snippet", {}).get("channelTitle"),
            })

    return data

def upload_data_to_d1(data):
    url = os.environ.get("d1_url")

    response = requests.post(url, json=data)

    if response.status_code == 200:
        print("Data uploaded successfully:", response.text)
    else:
        print("Failed to upload data:", response.status_code, response.text)

def load_env():
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)

def main():
    load_env()

    start_time = datetime.now()
    print(f"Process started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    url = os.environ.get("schedule_url")
    timezone = os.environ.get("timezone")

    response = scrape_website(url, timezone)
    data = process_data(response, timezone)

    upload_data_to_d1(data)

    end_time = datetime.now()
    print(f"Process ended at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    duration = end_time - start_time
    print(f"Total duration: {format_duration(duration)}")


if __name__ == "__main__":
    main()