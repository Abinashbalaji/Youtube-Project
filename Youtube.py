import googleapiclient.discovery
from pprint import pprint
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
import streamlit as st

api_service_name = "youtube"
api_version = "v3"

api_key = 'AIzaSyDzGgL4YXbbW7r2ljTAsRjF9mDYCfrl7KM'
c_id="UCJcCB-QYPIBcbKcBQOTwhiA"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey = api_key)

def channel_details(c_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=c_id
    )
    response = request.execute()

    data = {"Channel_id" : c_id,
            "Channel_Name" : response['items'][0]['snippet']['localized']['title'],
            "Published_at" : response['items'][0]['snippet']['publishedAt'],
            "Subsribers_count" : response['items'][0]['statistics']['subscriberCount'],
            "Video_count" : response['items'][0]['statistics']['videoCount'],
            "View_count" : response['items'][0]['statistics']['viewCount'],
            "Description" : response['items'][0]['snippet']['description'],
            "Playlist_id" : response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return data

def get_video_ids(c_id):
    
    request1 = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=c_id
    )
    response1 = request1.execute()
    p_id=response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    Video_ids = []
    Page_Token=None
    while True:
        request = youtube.playlistItems().list(
        part="snippet",
        maxResults= 50,
        pageToken=Page_Token,
        playlistId= p_id)
        response = request.execute()
        for i in range(len(response['items'])):
            Video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
        Page_Token=response.get('nextPageToken')

        if Page_Token is None:
            break
    
    return Video_ids

def video_details(Video_ids):
    Video_Details=[]
    for items in Video_ids:
        request_2 = youtube.videos().list(
            part="snippet,contentDetails,statistics",id = items)
        response_2 = request_2.execute()
    
        data_1 = {'Channel_name' : response_2['items'][0]['snippet']['channelTitle'],
                'Video_id' : response_2['items'][0]['id'],
                'Channel_id' : response_2['items'][0]['snippet']['channelId'],
                'Video_title' : response_2['items'][0]['snippet']['localized']['title'],
                'Video_description' : response_2['items'][0]['snippet']['localized']['title'],
                'Likes_count' : response_2['items'][0]['statistics']['likeCount'],
                'Views_count' : response_2['items'][0]['statistics']['viewCount'],
                'Comment_count' : response_2['items'][0]['statistics']['commentCount'],
                'Favourite_count' : response_2['items'][0]['statistics']['favoriteCount'],
                'Published_at' : response_2['items'][0]['snippet']['publishedAt'],
                'Duration' : int(pd.Timedelta(response_2['items'][0]['contentDetails']['duration']).total_seconds()),
                'Thumbnails' : response_2['items'][0]['snippet']['thumbnails']['default']['url']
            
        }
        Video_Details.append(data_1)
    return Video_Details

def get_comments(Video_ids):
    comment_Det=[]
    try:
        for videoid in Video_ids:
            request_3 = youtube.commentThreads().list(
            part="snippet",
            videoId=videoid,
            maxResults=20)
            
            response_3 = request_3.execute()
        
            for i in range(len(response_3['items'])):
                data_2 = {'Video_id' : response_3['items'][i]['snippet']['videoId'],
                        'Comment_id' : response_3['items'][i]['snippet']['topLevelComment']['id'],
                        'Comment_published_at' : response_3['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                        'Comment' : response_3['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay']}
                comment_Det.append(data_2)

    except:
        pass
    return comment_Det


uri = "mongodb+srv://abinash:2615@cluster0.fnzb9fj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

db = client.Youtube_project

def Channel_Details(c_id):
    
    Ch_details = channel_details(c_id)
    Vi_id = get_video_ids(c_id)
    Vi_details = video_details(Vi_id)
    Cmt_details = get_comments(Vi_id)

    collection_1 = db['Harvest']
    collection_1.insert_one({"Channel_details":Ch_details,"Video_details":Vi_details,"Comment_details":Cmt_details})

    return "Fetched Successfully"

import mysql.connector

mydb = mysql.connector.connect(

  host="localhost",
  user="root",
  password="",database ="Youtube"
 
)

mycursor=mydb.cursor(buffered=True)

def Channel_table():
    try:
        Create_query = """CREATE TABLE Channel_data (Channel_id VARCHAR(40), Channel_Name VARCHAR(20),
                                                    Published_at TIMESTAMP, Subsribers_count INT,Video_count INT, 
                                                    View_count INT, Description TEXT)"""
        mycursor.execute(Create_query)
        mydb.commit()
    except:
        "Table Already Created"

    ch_list = []
    db = client.Youtube_project
    collection_1 = db['Harvest']
    for data in collection_1.find({"Channel_details.Channel_Name": Channel_names}):
        ch_list.append(data["Channel_details"])
        
    df=pd.DataFrame(ch_list) 

    for index,row in df.iterrows():
        Insert_query = """INSERT INTO Channel_data (Channel_id, Channel_Name, 
                        Published_at,Subsribers_count, Video_count, View_count, Description)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)"""
        
        VALUES = (
            row["Channel_id"],
            row["Channel_Name"],
            row["Published_at"],
            row["Subsribers_count"],
            row["Video_count"],
            row["View_count"],
            row["Description"])
        try:
            mycursor.execute(Insert_query,VALUES)
            mydb.commit()
        except:
            pass

def Video_table():
    try:
        Create_query = ("CREATE TABLE Video_data (Channel_name VARCHAR(30), Video_id VARCHAR(15), Channel_id VARCHAR(30),Video_title VARCHAR(90), Video_description VARCHAR(250), Likes_count INT, Views_count INT, Comment_count INT, Favourite_count INT,Published_at TIMESTAMP, Duration INT, Thumbnails VARCHAR(250))")

        mycursor.execute(Create_query)
        mydb.commit()
        
    except:
        "Table Already Created" 

    vi_list = []
    db = client.Youtube_project
    collection_1 = db['Harvest']
    for data in collection_1.find({"Channel_details.Channel_Name": Channel_names }):
        for i in range(len(data["Video_details"])):
            vi_list.append(data["Video_details"][i])

    df=pd.DataFrame(vi_list) 

    for index,row in df.iterrows():
        
        Insert_query = ("INSERT INTO Video_data (Channel_name,Video_id, Channel_id, Video_title,Video_description, Likes_count, Views_count, Comment_count,Favourite_count,Published_at,Duration,Thumbnails) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        
        VALUES = (
            row["Channel_name"],
            row["Video_id"],
            row["Channel_id"],
            row["Video_title"],
            row["Video_description"],
            row["Likes_count"],
            row["Views_count"],
            row["Comment_count"],
            row["Favourite_count"],
            row["Published_at"],
            row["Duration"],
            row["Thumbnails"]
            
                
        )
        try:
            mycursor.execute(Insert_query,VALUES)
            mydb.commit()
        except:
            pass
        

def Comment_table():
    try:
        Create_query = ("CREATE TABLE Comment_data (Video_id VARCHAR(15), Comment_id VARCHAR(30),Comment_published_at TIMESTAMP,Comment VARCHAR(250))")

        mycursor.execute(Create_query)
        mydb.commit()
        
    except:
        "Table Already Created"

    co_list = []
    db = client.Youtube_project
    collection_1 = db['Harvest']
    for data in collection_1.find({"Channel_details.Channel_Name": Channel_names}):
        for i in range(len(data["Comment_details"])):
            co_list.append(data["Comment_details"][i])

    df=pd.DataFrame(co_list) 

    for index,row in df.iterrows():
        
        Insert_query = ("INSERT INTO Comment_data (Video_id, Comment_id, Comment_published_at,Comment) VALUES (%s,%s,%s,%s)")
        
        VALUES = (
            row["Video_id"],
            row["Comment_id"],
            row["Comment_published_at"],
            row["Comment"]
                
        )
        try:
            mycursor.execute(Insert_query,VALUES)
            mydb.commit()
        except:
            pass

st.header("YouTube Data Harvesting and Warehousing")
Selects = st.sidebar.radio("Navigation",["Home","Queries","Data Transfer","Migrage"])


def tables():

    Channel_table()
    Video_table()
    Comment_table()
    return "Table Inserted"

def view_channel_table():
    ch_list = []
    db = client.Youtube_project
    collection_1 = db['Harvest']
    for data in collection_1.find({"Channel_details.Channel_Name": Channel_names}):
        ch_list.append(data["Channel_details"])
        
    df=st.dataframe(ch_list) 
    return df

def view_video_table():
    vi_list = []
    db = client.Youtube_project
    collection_1 = db['Harvest']
    for data in collection_1.find({"Channel_details.Channel_Name": Channel_names}):
        for i in range(len(data["Video_details"])):
            vi_list.append(data["Video_details"][i])

    df=st.dataframe(vi_list) 
    return df

def view_comment_table():
    co_list = []
    db = client.Youtube_project
    collection_1 = db['Harvest']
    for data in collection_1.find({"Channel_details.Channel_Name": Channel_names}):
        for i in range(len(data["Comment_details"])):
            co_list.append(data["Comment_details"][i])

    df=st.dataframe(co_list)
    return df

if Selects == "Data Transfer":
    c_id = st.text_input("Enter the channel ID")
    c_ids = c_id.split(',')
    
    if st.button("Press to store"):
        for channel in c_ids:
            ch_list = []
            db = client.Youtube_project
            collection_1 = db['Harvest']
            for data in collection_1.find({},{"_id":0,"Channel_details":1}):
                ch_list.append(data["Channel_details"]["Channel_id"])
            if channel in ch_list:
                st.success("Channel details of the given channel id: " + channel + " already exists")
            else:
                output = Channel_Details(channel)
                st.success(output)

if Selects == "Migrage":
    def connect():
        uri = "mongodb+srv://abinash:2615@cluster0.fnzb9fj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

        client = MongoClient(uri, server_api=ServerApi('1'))
        db = client.Youtube_project
        collection = db.Harvest
        return collection
    def retrive():
        data = connect()
        ch_name_list = [i["Channel_details"]["Channel_Name"] for i in data.find({})]
        return ch_name_list
    
    Channel_names = st.selectbox("Select the channel:",retrive())

    if st.button("Migrate to Sql"):
        tables = tables()
        st.success(tables)
    
    small_table = st.sidebar.radio("[Select the table]",("Channels","Videos","comments"))

    if small_table == "Channels":
        view_channel_table()
    elif small_table == "Videos":
        view_video_table()
    elif small_table == "comments":
        view_comment_table()


if Selects == "Queries":
    quetions = st.selectbox("Pls select",
                            ('1. Video names with channel names',
                             '2. Most no.of Videos having channels and their videos count',
                             '3. Top 10 most viewed videos and their respective channels',
                             '4. Comments counts of each videos and its video names',
                             '5. Highest likes having video names and their channel names',
                             '6. Video names and its likes count',
                             '7. Channel names and its total views',
                             '8. Names of all the channels that have published videos in the year 2022',
                             '9. Average Duration of all videos and their channel names',
                             '10. Highest comment videos and thier channel names'
                             
                             ))

    if quetions == '1. Video names with channel names':

        query = "SELECT Channel_Name, Video_title FROM video_data"
        mycursor.execute(query)
        OT= mycursor.fetchall()

        st.write(pd.DataFrame(OT, columns = ['Channel_name','Video_title']))

    if quetions == '2. Most no.of Videos having channels and their videos count':

        query = "SELECT Channel_Name, Video_count FROM channel_data ORDER BY Video_count"
        mycursor.execute(query)
        OT= mycursor.fetchall()

        st.write(pd.DataFrame(OT, columns = ['Channel_Name','Video_count']))

    if quetions == '3. Top 10 most viewed videos and their respective channels':

        query = "SELECT Channel_Name,Video_title,Views_count FROM video_data ORDER BY Views_count"
        mycursor.execute(query)
        OT= mycursor.fetchall()

        st.write(pd.DataFrame(OT, columns = ['Channel_Name','Video_title','Views_count']))

    if quetions == '4. Comments counts of each videos and its video names':

        query = "SELECT Channel_name,Video_title,Comment_count FROM video_data ORDER BY Comment_count"
        mycursor.execute(query)
        OT= mycursor.fetchall()

        st.write(pd.DataFrame(OT, columns = ['Channel_Name','Video_title','Comment_count']))

    if quetions == '5. Highest likes having video names and their channel names':

        query = "SELECT Channel_name,Video_title,Likes_count FROM video_data ORDER BY Likes_count"
        mycursor.execute(query)
        OT= mycursor.fetchall()

        st.write(pd.DataFrame(OT, columns = ['Channel_Name','Video_title','Likes_count']))

    if quetions == '6. Video names and its likes count':

        query = "SELECT Video_title,Likes_count FROM video_data ORDER BY Likes_count"
        mycursor.execute(query)
        OT= mycursor.fetchall()

        st.write(pd.DataFrame(OT, columns = ['Video_title','Likes_count']))
    
    if quetions == '7. Channel names and its total views':

        query = "SELECT Channel_name,View_count FROM channel_data ORDER BY View_count"
        mycursor.execute(query)
        OT= mycursor.fetchall()

        st.write(pd.DataFrame(OT, columns = ['Channel_name','View_count']))

    if quetions == '8. Names of all the channels that have published videos in the year 2022':

        query = """SELECT Channel_name,Video_title,Published_at FROM video_data where extract(year FROM Published_at) = 2022;"""
        mycursor.execute(query)
        OT= mycursor.fetchall()

        st.write(pd.DataFrame(OT, columns = ['Channel_name','Video_title','Published_at']))

    if quetions == '9. Average Duration of all videos and their channel names':

        query = "SELECT Channel_name,AVG(Duration) AS average_duration FROM video_data GROUP BY Channel_name"
        mycursor.execute(query)
        OT= mycursor.fetchall()

        st.write(pd.DataFrame(OT, columns = ['Channel_name','average_duration']))

    if quetions == '10. Highest comment videos and thier channel names':

        query = "SELECT Channel_name,Video_title,Comment_count FROM video_data ORDER BY Comment_count"
        mycursor.execute(query)
        OT= mycursor.fetchall()

        st.write(pd.DataFrame(OT, columns = ['Channel_name','Video_title','Comment_count']))