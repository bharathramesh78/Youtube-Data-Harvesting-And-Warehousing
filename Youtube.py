import streamlit as st
from googleapiclient.discovery import build
from pymongo import MongoClient
import mysql.connector

# Function to connect to the YouTube API and retrieve channel data
def get_channel_data(api_key, channel_id):
    youtube = build("youtube", "v3", developerKey=api_key)
    request = youtube.channels().list(part="snippet,statistics", id=channel_id)
    
    try:
        response = request.execute()
        items = response.get("items", [])
        
        if items:
            return items[0]
        else:
            st.error("No channel data found.")
            return None
    except Exception as e:
        st.error(f"Error retrieving channel data: {e}")
        return None
        
# Function to store data in MongoDB
def store_in_mongodb(data):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["youtube_data"]
    collection = db["channel_data_bharath"]
    collection.insert_one(data)

# Function to migrate data to MySQL
def migrate_to_sql(data):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Rmba7898@",
        database="youtube_data_warehouse",
        auth_plugin='mysql_native_password'
    )
    cursor = conn.cursor()

    # Create tables if not exist
    cursor.execute("create table if not exists channel(channel_id varchar(255),channel_name varchar(255),channel_type varchar(255),channel_views int,channel_Descirption text,channel_Status varchar(255),primary key (channel_id));")
    cursor.execute("create table if not exists playlist(playlist_id varchar(255),channel_id varchar(255),playlist_name varchar(255),primary key (playlist_id),foreign key (channel_id) references channel (channel_id));")
    cursor.execute("create table if not exists video(video_id varchar(255),playlist_id varchar(255),video_name varchar(255),video_Description text,published_date datetime,view_count int,like_count int,dislike_count int,favorite_count int,comment_count int,duration int,thumbnail varchar(255),caption_status varchar(255),primary key (video_id),foreign key (playlist_id) references playlist (playlist_id));")
    cursor.execute("create table if not exists comment(comment_id varchar(255),video_id varchar(255),comment_text text,comment_author varchar(255),comment_published_date datetime,primary key(comment_id),foreign key(video_id) references video (video_id));")

    # Insert data into MySQL tables
    query = "INSERT INTO channel(channel_id, channel_name, channel_type, channel_views, channel_Descirption, channel_Status) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(query, (data["Channel_Name"]["Channel_Id"], data["Channel_Name"]["Channel_Name"], data["Channel_Name"]["Channel_Description"], data["Channel_Name"]["Channel_Views"], data["Channel_Name"]["Channel_Description"]))

    query = "INSERT INTO playlist(playlist_id, channel_id, playlist_name) VALUES (%s, %s, %s)"
    cursor.execute(query, (data["Channel_Name"]["Playlist_Id"], data["Channel_Name"]["Channel_Id"], data["Channel_Name"]["Playlist_Name"]))

    query = "INSERT INTO video(video_id, playlist_id, video_name, video_description, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail_count, caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(query, (data["Video_Id_1"]["Video_Id"], data["Channel_Name"]["Playlist_Id"], data["Video_Id_1"]["Video_Name"], data["Video_Id_1"]["Video_Description"], data["Video_Id_1"]["PublishedAt"], data["Video_Id_1"]["View_Count"], data["Video_Id_1"]["Like_Count"], data["Video_Id_1"]["Dislike_Count"], data["Video_Id_1"]["Favorite_Count"], data["Video_Id_1"]["Comment_Count"], data["Video_Id_1"]["Duration"], data["Video_Id_1"]["Thumbnail"], data["Video_Id_1"]["Caption_Status"]))

    query = "INSERT INTO comment(comment_id, video_id, comment_text, comment_author, comment_published_date) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(query, (data["Video_Id_1"]["Comments"]["Comment_Id_1"]["Comment_Id"], data["Video_Id_1"]["Video_Id"], data["Video_Id_1"]["Comments"]["Comment_Id_1"]["Comment_Text"], data["Video_Id_1"]["Comments"]["Comment_Id_1"]["Comment_Author"], data["Video_Id_1"]["Comments"]["Comment_Id_1"]["Comment_PublishedAt"]))


    conn.commit()
    cursor.close()
    conn.close()
    
# Function to query MySQL database
def query_sql_database(channel_name):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Rmba7898@",
        database="youtube_data_warehouse"
    )
    cursor = conn.cursor()

    # Execute SQL query to retrieve data
    
    
    st.write(f"Names of all the videos and their corresponding channels:")
    cursor.execute(f"SELECT v.video_name, c.channel_name FROM video v JOIN playlist p ON v.playlist_id = p.playlist_id JOIN channel c ON p.channel_id = c.channel_id;")
   
   st.write(f"Channels with most number of videos and it's count")
    cursor.execute(f"SELECT c.channel_name, COUNT(v.video_id) AS video_count FROM channel c JOIN playlist p ON c.channel_id = p.channel_id JOIN video v ON p.playlist_id = v.playlist_id GROUP BY c.channel_name ORDER BY video_count DESC LIMIT 1;")
   
   st.write(f"Top 10 most viewed videos and their corresponding channels")
    cursor.execute(f"SELECT v.video_name, c.channel_name, v.view_count FROM video v JOIN playlist p ON v.playlist_id = p.playlist_id JOIN channel c ON p.channel_id = c.channel_id ORDER BY v.view_count DESC LIMIT 10;")
   
   st.write(f"Number of comments on each video and their corresponding video names")
    cursor.execute(f"SELECT v.video_name, COUNT(c.comment_id) AS comment_count FROM video v LEFT JOIN comment c ON v.video_id = c.video_id GROUP BY v.video_name;")
    
    st.write(f"Videos with highest numebr of likes and their corresponding channel name")
    cursor.execute(f"SELECT v.video_name, c.channel_name, v.like_count FROM video v JOIN playlist p ON v.playlist_id = p.playlist_id JOIN channel c ON p.channel_id = c.channel_id ORDER BY v.like_count DESC LIMIT 1;")
    
    st.write(f"Total number of likes and dislikes on each video and their corresponding video names")
    cursor.execute(f"SELECT v.video_name, SUM(v.like_count) AS total_likes, SUM(v.dislike_count) AS total_dislikes FROM video v GROUP BY v.video_name;")
    
    st.write(f"Total number of views on each channels and their corresponding channel names")
    cursor.execute(f"SELECT c.channel_name, SUM(v.view_count) AS total_views FROM channel c JOIN playlist p ON c.channel_id = p.channel_id JOIN video v ON p.playlist_id = v.playlist_id GROUP BY c.channel_name;")
    
    st.write(f"Number of all channels that have published video in the year 2022")
    cursor.execute(f"SELECT DISTINCT c.channel_name FROM channel c JOIN playlist p ON c.channel_id = p.channel_id JOIN video v ON p.playlist_id = v.playlist_id WHERE YEAR(v.published_date) = 2022;")
    
    st.write(f"Average duration of all videos in each channel and their corresponding channel names:")
    cursor.execute(f"SELECT c.channel_name, AVG(v.duration) AS average_duration FROM channel c JOIN playlist p ON c.channel_id = p.channel_id JOIN video v ON p.playlist_id = v.playlist_id GROUP BY c.channel_name;")
    
    st.write(f"Videos with the highest number of comments and their corresponding channel names:")
    cursor.execute(f"SELECT v.video_name, c.channel_name, COUNT(com.comment_id) AS comment_count FROM video v JOIN playlist p ON v.playlist_id = p.playlist_id JOIN channel c ON p.channel_id = c.channel_id LEFT JOIN comment com ON v.video_id = com.video_id GROUP BY v.video_name, c.channel_name ORDER BY comment_count DESC LIMIT 1;")
   
    result = cursor.fetchall()

    cursor.close()
    conn.close()

    return result

# Streamlit UI code
st.title("YouTube Data Analysis App")

# Input for YouTube channel ID
channel_id = st.text_input("Enter YouTube Channel ID:")

# Button to retrieve and display data
if st.button("Retrieve Data"):
    # Call YouTube API and display data
    channel_data = get_channel_data(api_key="AIzaSyCJz1UmGimO8eODYKgPfb240yvnLyVhFeY", channel_id=channel_id)

    # Store in MongoDB
    store_in_mongodb({channel_data})


    # Migrate data to MySQL
    migrate_to_sql(channel_data)

# Display retrieved data
if st.button("Display Data"):
    channel_name = st.text_input("Enter Channel Name:")
    data = query_sql_database(channel_name)

    st.write(f"## {channel_name} Data")
    st.table(data)
