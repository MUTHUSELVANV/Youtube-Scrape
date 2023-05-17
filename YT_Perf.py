import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import pandas as pd
import seaborn as sns
import numpy as np
from pymongo import MongoClient

st.set_page_config(page_title='YT Scrapper')

# Streamlit page display

st.header('YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit')


n = st.number_input('Enter the number of youtube data to be Searched: (MAX=10)')
while (n>10):
    st.error('Error! You can search only upto 10 Youtube channel')
    n = st.number_input('Enter the number of youtube data to be Searched: (MAX=10)')
if(n.is_integer()==False):
    st.error('Error! Enter an Integer from 1-10')
    n = st.number_input('Enter the number of youtube data to be Searched: (MAX=10)')
n=round(n)
st.write('the Number of channels to be searched is',n)

channel_id = []

#x=st.text_input(label='love')

for i in range(n):
    if i == 0:
        st.write(i+1,'st Channel ID')
        channel_id.append(st.text_input(label='Enter Below',key=i))
    elif i == 1:
        st.write(i+1,'nd Channel ID')
        channel_id.append(st.text_input(label='Enter Below',key=i))
    elif i == 2:
        st.write(i+1,'rd Channel ID')
        channel_id.append(st.text_input(label='Enter Below',key=i))
    else:
        st.write(i+1,'th Channel ID')
        channel_id.append(st.text_input(label='Enter Below',key=i))

submit = st.button('Submit')  
if submit:
    channel_id = pd.DataFrame(channel_id)
    #st.success('Succesfully Added Channel IDs')
    st.success(f"Succesfully Added {len(channel_id)} Channel IDs ")
st.markdown('_Channel ID Entered shown Below_')
st.dataframe(channel_id)
#channel_id=channel_id.values.tolist()



#api_key ='AIzaSyBGDgcEWkoJn6ZLyTu0g7_P7ziagQDIjZQ'
#api_key ='AIzaSyCaaoxSkYn_Pq_7KC5DGFlvXk0elqQiUKg'
api_key='AIzaSyDWMR_rH6-nf_ebBKaK6iD-xGFYyGG1fhM'
#api_key ='AIzaSyDv3L3_VeV3NwOQqAQVq1C_j0QC3PG5CcA'
#api_key ='AIzaSyBGvhoZsiYdBytd2LuG8bzqpvMBdCLNRx4'
c_ids= channel_id
#c_ids=c_ids.values.tolist()

#youtube = googleapiclient.discovery.build(api_service_name, api_version, credentials=credentials)
youtube = build('youtube','v3',developerKey=api_key) 


def get_channel_stats(youtube,c_ids):
    
    all_data=[]
    
    # Raise a request to youtube server 
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=','.join(c_ids)
    )
    response= request.execute() # response is a dictionary returned from the fn execute
    
    # Storing the specific results from response into a dictionary for multiple channels
    for i in range(len(response['items'])):
        data = {'channel_name': response['items'][i]['snippet']['title'],
                'channel_Description': response['items'][i]['snippet']['description'],
                'subs_count' : response['items'][i]['statistics']['subscriberCount'],
                'views' : response['items'][i]['statistics']['viewCount'],
                'tot_vid' : response['items'][i]['statistics']['videoCount'],
                'playlist_id' : response['items'][i]['contentDetails']['relatedPlaylists']['uploads']
            }
        all_data.append(data)
    
    return all_data

c_stat = get_channel_stats(youtube,c_ids)
c_data = pd.DataFrame(c_stat)
c_data['subs_count']=pd.to_numeric(c_data['subs_count'])
c_data['views']=pd.to_numeric(c_data['views'])
c_data['tot_vid']=pd.to_numeric(c_data['tot_vid'])
titlee=c_data['channel_name']



playlist_id = c_data['playlist_id'].iloc[0:len(c_data)]
playlist_id=list(playlist_id)


def get_video_ids(youtube,playlist_id):
    
    request = youtube.playlistItems().list(
        part = 'contentDetails',
        playlistId = playlist_id,
        maxResults =50 # Shows 1st 50 playlist id's 
    )
    response = request.execute() # response has all the details related to videoId
    
    # Now, we are going to get all the video_ID from the playlist_Id (not only 50)
    video_ids =[]
    
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
    
    next_page_token = response.get('nextPageToken') # used get method instead of response['nextPageToken'] since .get doesnt give error but [] gives error if false
    more_pages = True
    
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part = 'contentDetails',
                playlistId = playlist_id,
                maxResults =50, # Shows 1st 50 playlist id's 
                pageToken = next_page_token
            )
            response = request.execute()
            
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            
            next_page_token = response.get('nextPageToken')
        
    return video_ids


v_id=[]
for i in range(len(playlist_id)):
    v_id.append(get_video_ids(youtube,playlist_id[i]))

alan = pd.DataFrame(list(zip(playlist_id,v_id)))
alan.columns=['Playlist','VideoID']




def get_video_details(youtube, video_ids):
    all_video_stats = []
    
    for i in range(0,len(video_ids),50):
        request = youtube.videos().list(
            part = 'snippet,statistics',
            id =','.join(video_ids[i:i+50]) # According to youtube guidelines, we can pass only 50 videos at a time 
        )
        response = request.execute()
    
        for video in response['items']:
            video_stats = dict(Title = video['snippet']['title'],
                            Published_date = video['snippet']['publishedAt'],
                            Views = video['statistics']['viewCount'],
                            likes = video['statistics'].get('likeCount',0),
                            #DisLikes = video['statistics']['dislikeCount'], - Dislike has been blocked by YouTube Guidelines
                            Comments = video['statistics'].get('commentCount',0),
                            VideoID = video['id']
                            )
            all_video_stats.append(video_stats)
    return all_video_stats



video_details=[]
for i in range(len(v_id)):
    video_details.append(get_video_details(youtube, v_id[i]))
video_data = pd.DataFrame(video_details)
video_data = video_data.T

# Function to extract dictionary values
def extract_dict_value(dictionary, key):
    if isinstance(dictionary, dict) and key in dictionary:
        return dictionary[key]
    else:
        return None

# Create a new dataframe with references to dictionary values
tit1 = video_data.applymap(lambda cell: extract_dict_value(cell, 'Title'))
videoID = video_data.applymap(lambda cell: extract_dict_value(cell, 'VideoID'))
pd1 = video_data.applymap(lambda cell: extract_dict_value(cell, 'Published_date'))
v1 = video_data.applymap(lambda cell: extract_dict_value(cell, 'Views'))
likes = video_data.applymap(lambda cell: extract_dict_value(cell, 'likes'))
c1 = video_data.applymap(lambda cell: extract_dict_value(cell, 'Comments'))
tit1.replace(np.nan,0) 
videoID.replace(np.nan,0)
pd1.replace(np.nan,0) 
v1.replace(np.nan,0) 
c1.replace(np.nan,0)
tit1 = {'title':tit1,
        'VideoID':videoID,
        'published Date':pd1,
        'Views':v1,
        'Likes':likes,
        'Comments':c1
    }
Vid_con=[]
#tit1['title'][0] #= pd.DataFrame(tit1) 
for i in range(len(v_id)):
    Vid_con.append(pd.DataFrame({
        'title': tit1['title'][i],
        'VideoID':tit1['VideoID'][i],
        'published Date': tit1['published Date'][i],
        'Views': tit1['Views'][i],
        'Likes': tit1['Likes'][i],
        'Comments': tit1['Comments'][i]
    }))


for i in range(len(v_id)):
    Vid_con[i].dropna(inplace = True)


col1,col2 = st.columns(2)
with col1:
    st.subheader('Channel Table')
    st.dataframe(c_data)
with col2:
    st.subheader('Video_ID Table')
    st.dataframe(alan)
#col3,col4 = st.columns(2)
#with col3:
    #st.subheader('Video Details Table')
    #for i in range(len(v_id)):
        #st.dataframe(just[i])
#with col4:
    #st.write('Alan')


st.subheader('Video Details Table')
for i in range(len(v_id)):
    st.write(c_data['channel_name'][i])
    st.dataframe(Vid_con[i])


st.subheader('Comments Table')

def get_com(youtube, video_ids):
    cmt = []

    request = youtube.commentThreads().list(
        part='snippet',
        videoId=video_ids,
        maxResults=50
    )
    response = request.execute()

    for i in range(len(response['items'])):
        ct = {
            'Video_ID': response['items'][i]['snippet'].get('videoId', 0),
            'Comments': response['items'][i]['snippet']['topLevelComment']['snippet'].get('textDisplay', None)

        }
        cmt.append(ct)

    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_ids,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for i in range(len(response['items'])):
                ct = {
                    'Video_ID': response['items'][i]['snippet'].get('videoId', 0),
                    'Comments': response['items'][i]['snippet']['topLevelComment']['snippet'].get('textDisplay', None)
                }
                cmt.append(ct)

            next_page_token = response.get('nextPageToken')

    return cmt


comments = []
for i in range(len(v_id)):
    cntr=[]
    for j in range(len(v_id[i])):
        #current_comments = get_com(youtube, v_id[0][i])
        cntr.append(get_com(youtube,v_id[i][j]))
    comments.append(cntr)

finl=[]
for i in range(len(v_id)):
    
    commen=comments[i]
    commen=pd.DataFrame(commen)
    comme=commen.T

    dfs = []
    for j in range(comme.shape[1]):
        okk = []
        okk=comme[j]
        okk=pd.DataFrame(okk)
        okk.dropna(inplace = True)

        
        # Initialize an empty dictionary to store the new column values
            
        new_columns = {}

        # Iterate over each column in the original DataFrame
        for column in okk.columns:
            # Iterate over each row in the column
            for k, row in enumerate(okk[column]):
                # Iterate over each key-value pair in the dictionary
                for key, value in row.items():
                    # Create a new column name using the key
                    new_column_name = f'{key}'
                    # If the new column name doesn't exist in the dictionary, initialize it with an empty list
                    if new_column_name not in new_columns:
                        new_columns[new_column_name] = []
                    #Append the corresponding value to the new column
                    new_columns[new_column_name].append(value)

            # Create the new DataFrame from the dictionary
            new_df = pd.DataFrame(new_columns)
        # Append the new DataFrame to the list
        dfs.append(new_df)
    finl.append(dfs)


tyre=pd.DataFrame(finl)

tyre=tyre.T
tyre.columns = titlee
st.dataframe(tyre)


client = MongoClient('mongodb://localhost:27017/')
db = client['YT_Scrape'] 

dbtitle=titlee


option=["yes","no"]
name = st.selectbox('Are you done with extracting the Youtube Data', option, index=1)
sk=1

if name == "yes":
    
    st.cache_data
    def get_options():
        return dbtitle
    # Fetch the options using the get_options function
    options = get_options()
    sk+=1

    st.header('Now, We are going to export data to MongoDB')

    st.dataframe(dbtitle)
    opt = st.selectbox('Select the channel id to be exported to MongoDB',options)

    submit1 = st.checkbox("Export now ") 

    if submit1:

        selected_channel = c_data[c_data['channel_name'] == opt].iloc[0]

        channel_data = {
        'channel_name': selected_channel['channel_name'],
        'subs_count': selected_channel['subs_count'],
        'tot_vid': selected_channel['tot_vid'],
        'playlist_id': selected_channel['playlist_id']
        }
        selected_VID = alan[alan['Playlist'] == selected_channel['playlist_id']].iloc[0]

        ixx=[]
        for i in range(len(playlist_id)):
            if (Vid_con[i]['VideoID'][0]==selected_VID['VideoID'][0]):
                ixx=i



        video_detl={}
        for i in range(len(Vid_con[ixx])):
            video_detl[i] = {
            'Video Title': Vid_con[ixx]['title'][i],
            'Video ID': Vid_con[ixx]['VideoID'][i],
            'Published':Vid_con[ixx]['published Date'][i],
            'Views': Vid_con[ixx]['Views'][i],
            'Likes': Vid_con[ixx]['Likes'][i],
            'Comments': Vid_con[ixx]['Comments'][i]
            }

        exp = {'Channel Details': channel_data,
            'Video Details' : video_detl
                }
        st.write(exp)

        client = MongoClient('mongodb://localhost:27017/')
        db = client['YT_Scrape'] 


        #Adding Channel ID
        collection = db['Channed_ID']

        def convert_numpy_int64(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, dict):
                return {str(key): convert_numpy_int64(value) for key, value in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [convert_numpy_int64(item) for item in obj]
            return obj

        converted_exp = convert_numpy_int64(exp)
        collection.insert_one(converted_exp)
        # Check if the insertion was successful
        st.success('Channel data inserted successfully!')

        yn=["Yes","No"]
        kit =st.selectbox('Do you want to add more channels to MongoDB',yn,index=1)

        while kit=='Yes':
            st.header('Moving on, We are going to futher export data to MongoDB')
            sk+=1

            col2=dbtitle.copy()
            col2 = dbtitle.drop(dbtitle[dbtitle == opt].index)
            dbtitle = dbtitle.drop(dbtitle[dbtitle == opt].index)

            st.cache_data
            def get_options():
                return col2

            # Fetch the options using the get_options function
            options = get_options()



            st.dataframe(col2)
            opt = st.selectbox('Select the channel id to be exported to MongoDB',options,key=sk+100)


            submit2 = st.checkbox("Export Now" ,key=sk+500)

            if submit2:
                
                selected_channel = c_data[c_data['channel_name'] == opt].iloc[0]

                channel_data = {
                'channel_name': selected_channel['channel_name'],
                'subs_count': selected_channel['subs_count'],
                'tot_vid': selected_channel['tot_vid'],
                'playlist_id': selected_channel['playlist_id']
                }
                selected_VID = alan[alan['Playlist'] == selected_channel['playlist_id']].iloc[0]

                ixx=[]
                for i in range(len(playlist_id)):
                    if (Vid_con[i]['VideoID'][0]==selected_VID['VideoID'][0]):
                        ixx=i


                video_detl={}
                for i in range(len(Vid_con[ixx])):
                    video_detl[i] = {
                    'Video Title': Vid_con[ixx]['title'][i],
                    'Video ID': Vid_con[ixx]['VideoID'][i],
                    'Published':Vid_con[ixx]['published Date'][i],
                    'Views': Vid_con[ixx]['Views'][i],
                    'Likes': Vid_con[ixx]['Likes'][i],
                    'Comments': Vid_con[ixx]['Comments'][i]
                    }

                exp = {'Channel Details': channel_data,
                    'Video Details' : video_detl
                        }
                st.write(exp)


                def convert_numpy_int64(obj):
                    if isinstance(obj, np.integer):
                        return int(obj)
                    elif isinstance(obj, dict):
                        return {str(key): convert_numpy_int64(value) for key, value in obj.items()}
                    elif isinstance(obj, (list, tuple)):
                        return [convert_numpy_int64(item) for item in obj]
                    return obj

                converted_exp = convert_numpy_int64(exp)
                collection.insert_one(converted_exp)
                # Check if the insertion was successful
                st.success('Channel data inserted successfully!')
                
                
            yn=["Yes","No"]
            kit = st.selectbox('Do you want to add more channels to MongoDB',yn, index=1,key=sk+1000)


            #if len(col2)==1 and name=="Yes":
            #    st.write('No more Data to be Exported')
            #    name=="No"
               
            #if(name=="No"):
            #    st.write('Since you pressed No, No more data to be exported')
                


            #if(len(opt)==0):
            #    st.write('No more Data to be Exported')
            #elif(name=="No"):
            #    st.write('Since you pressed No, No more data to be exported')