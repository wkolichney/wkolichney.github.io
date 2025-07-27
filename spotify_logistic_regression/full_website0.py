
'''

File Upload section

'''

import streamlit as st
import pandas as pd
import json
import requests
import base64
import urllib.parse
import pytz
import time
from datetime import datetime
from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.preprocessing import LabelEncoder
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pickle
import os
from supabase import create_client, Client
################################################################################################################################
st.title("Predicting your Spotify Skips with Logisitic Regression")

# Store data from the API Calls


SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
##################################################################################################################################
# youtube instructional video
st.header("How to Use This Site")
st.markdown("""
Watch this short video tutorial on how to upload your Spotify data and use the app:

[Watch the video on YouTube](https://www.youtube.com/watch?v=4XgMMRgV6OU)
""")

#########################################################################################################3
st.title("Upload Your Spotify JSON Files")

st.markdown("""
Click this link to request your listening data from Spotify:

(https://www.spotify.com/account/privacy/)
""")



# Upload multiple JSON files
uploaded_files = st.file_uploader(
    "For now, due to rate limiting on pulling genre data, only input two of of your most recent listening data .json files. Typically named something like 'Streaming_History_2024_11'",
    type="json", 
    accept_multiple_files=True
)

all_dfs = []

if uploaded_files:
    for uploaded_file in uploaded_files:
        try:
            # Load each file
            json_data = json.load(uploaded_file)

            # Convert to DataFrame
            if isinstance(json_data, list):
                df = pd.DataFrame(json_data)
            elif isinstance(json_data, dict):
                df = pd.DataFrame([json_data])
            else:
                st.warning(f"Unsupported JSON structure in {uploaded_file.name}")
                continue

            all_dfs.append(df)

        except Exception as e:
            st.error(f"Error loading {uploaded_file.name}: {e}")

    # Concatenate all DataFrames
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)

        # Drop sensitive columns if they exist
        columns_to_drop = ['ip_addr_decrypted', 'user_agent_decrypted', 'conn_country']
        combined_df.drop(columns=[col for col in columns_to_drop if col in combined_df.columns], inplace=True)

        st.session_state['raw_data'] = combined_df  # ✅ Save for later use
        st.success("All files loaded and combined!")
        st.dataframe(combined_df)



# SECTION 2: Spotify Developer Credentials
st.markdown("---")
st.header("Spotify Developer Credentials")


st.markdown("""
In order to get the genre for your songs, we will need to get you a Spotify API key.
            
### Step 1: Connect your Spotify account to web developer access
- Go to [Spotify Developer](https://developer.spotify.com/)
- Enter your Spotify log in information
### Step 2: Create app, which generates API key
- Go to [Spotify Dashboard](https://developer.spotify.com/dashboard)
- Select "Create app"
- Fill out required fields. The only thing that is important to fill specifically is the "Redirect URI". Simply copy and paste this website's URL into the space (https://wkolichneyapp-spotify-machine-learning.streamlit.app/)
- Hit save!
### Step 3: Get your Client ID and Client Secret keys
- Copy the "Client ID" and "Client Secret" fields from the app you just created. Navigate to "Basic Information", and copy the "Client ID". To access your secret ID, click "View client secret", just under your Client ID. 
- Paste that log in information into the respective fields below. 
- Hit your enter key on your computer.
            
""")


client_id = st.text_input("Client ID")
client_secret = st.text_input("Client Secret", type="password")
redirect_uri = "https://wkolichneyapp-spotify-machine-learning.streamlit.app/"
scope = "user-library-read user-top-read"

# STEP 4: Generate Authorization URL
if client_id and client_secret:
    st.markdown("""
    ### Step 4: Authorize Application: 
    - Click on the link below. This will open my website again, but in the address bar, you will see your token. It will look like... 
    - https: //wkolichneyapp-spotify-machine-learning.streamlit.app/?code=AQCWgfh80w...
    - Copy everything AFTER code=  (do not include "code" or the equal sign, just the full text to the right of them)
    - That will be your token used to collect genre data 
    - Paste that token into the code field below and hit enter.
                """)

    auth_url = (
        f"https://accounts.spotify.com/authorize"
        f"?client_id={client_id}"
        f"&response_type=code"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
        f"&scope={urllib.parse.quote(scope)}"
    )
    st.markdown(f"[Click here to authorize Spotify access]({auth_url})")
    auth_code = st.text_input("Step 2: Paste the 'code' from the URL after authorization")

    # STEP 5: Exchange Code for Access Token
    if auth_code:
        st.markdown("""
        ### Step 5: Get Access Token: 
        Nothing more needs to be done on your end! Thanks for sticking with the processs! If you see the token, then afterwars see an error message, do not worry. That token will stay active for ~1 hour.
                    """)
        token_url = "https://accounts.spotify.com/api/token"
        headers = {
            "Authorization": "Basic " + base64.b64encode(f"{client_id}:{client_secret}".encode()).decode(),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": redirect_uri,
        }
        response = requests.post(token_url, headers=headers, data=data)

        if response.status_code == 200:
            token_info = response.json()
            access_token = token_info.get("access_token")
            st.session_state['access_token'] = access_token
            st.success("Access token obtained and saved!")
            st.code(access_token)

        else:
            st.error(f"Failed to get access token: {response.status_code} - {response.text}")


#############################################################################################################################


# Ask the user for their timezone
st.markdown("---")
st.subheader("Choose Your Timezone")
timezone_options = pytz.all_timezones
user_timezone = st.selectbox("What region do you live in? (Used to convert timestamps)", options=timezone_options, index=timezone_options.index("America/New_York"))

if 'raw_data' in st.session_state and not st.session_state['raw_data'].empty:
    df = st.session_state['raw_data']
    # STEP 1: Select and rename relevant columns
    music = df[['ts', 'ms_played', 'master_metadata_track_name', 
                'master_metadata_album_artist_name', 'master_metadata_album_album_name',
                'reason_start', 'reason_end', 'shuffle', 'skipped', 'offline', 
                'spotify_track_uri', 'incognito_mode']].copy()

    music = music.rename(columns={
        'master_metadata_track_name': 'track_name',
        'master_metadata_album_artist_name': 'artist',
        'master_metadata_album_album_name': 'album_name'
    })

    # STEP 2: Convert timestamps to selected timezone
    def convert_to_user_timezone(df, tz_str):
        df = df.copy()
        df['ts'] = pd.to_datetime(df['ts'])
        if df['ts'].dt.tz is None:
            df['ts'] = df['ts'].dt.tz_localize('UTC').dt.tz_convert(tz_str)
        else:
            df['ts'] = df['ts'].dt.tz_convert(tz_str)
        return df

    music_ts = convert_to_user_timezone(music, user_timezone)
    st.success(f"Timestamps converted to: `{user_timezone}`")
    st.dataframe(music_ts.head())

    # STEP 3: Prepare distinct artist list
    distinct_artist_try = music_ts.drop_duplicates(subset=['artist'], keep='first')[['artist', 'spotify_track_uri']].sort_values('artist')


    st.markdown("### Processed Artist Data")
    st.write("Distinct Artists")
    st.dataframe(distinct_artist_try.head())


    # Optional: Save to session state if needed later
    st.session_state['music_ts'] = music_ts
    st.session_state['distinct_artists'] = distinct_artist_try



########################################################################################################################################################



def get_artist_genre_from_track_uri(track_uri, access_token, max_retries=3):
    """
    Fetch the genre of an artist using a Spotify track URI with retry logic.
    Returns 'rate_limited' if the artist should be retried later.
    """
    if pd.isna(track_uri) or not track_uri:
        return 'unknown'

    try:
        track_id = track_uri.split(":")[-1]
    except:
        return 'unknown'

    headers = {"Authorization": f"Bearer {access_token}"}

    for attempt in range(max_retries):
        try:
            # Step 1: Fetch track details to get artist ID
            track_url = f"https://api.spotify.com/v1/tracks/{track_id}"
            track_response = requests.get(track_url, headers=headers, timeout=10)

            if track_response.status_code == 200:
                track_data = track_response.json()
                if 'artists' in track_data and track_data['artists']:
                    artist_id = track_data["artists"][0]["id"]

                    # Step 2: Fetch artist details to get genres
                    artist_url = f"https://api.spotify.com/v1/artists/{artist_id}"
                    artist_response = requests.get(artist_url, headers=headers, timeout=10)

                    if artist_response.status_code == 200:
                        artist_data = artist_response.json()
                        genres = artist_data.get("genres", [])
                        return genres[0] if genres else 'unknown'
                    elif artist_response.status_code == 429:
                        retry_after = min(int(artist_response.headers.get("Retry-After", 1)), 60)
                        st.warning(f"Rate limited on artist request. Waiting {retry_after}s...")
                        time.sleep(retry_after)
                        continue

            elif track_response.status_code == 429:
                retry_after = min(int(track_response.headers.get("Retry-After", 1)), 60)
                print(f"Rate limited on track request. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            elif track_response.status_code == 404:
                return 'unknown'  # Track not found

        except requests.exceptions.RequestException as e:
            print(f"Request error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
        except Exception as e:
            print(f"Unexpected error: {e}")
            return 'unknown'

    return 'rate_limited'  # Give up after retries


@st.cache_data(show_spinner=False)
def get_existing_artists_in_supabase():
    try:
        response = supabase.table("artist_genres").select("artist").execute()
        if response.data:
            return set(row["artist"] for row in response.data)
        else:
            return set()
    except Exception as e:
        st.warning(f"⚠️ Could not load existing Supabase artists: {e}")
        return set()

def get_genres_for_unique_artists(music_df, access_token):
    """
    Get genres for unique artists using Supabase as the only memory source.
    Automatically skips artists that already exist in Supabase.
    """
    # Step 0: Load existing artist names from Supabase
    existing_artists = get_existing_artists_in_supabase()


    # Step 1: One unique URI per artist
    unique_artists = music_df.groupby('artist')['spotify_track_uri'].first().reset_index()
    unique_artists = unique_artists.dropna(subset=['artist', 'spotify_track_uri'])

    # Step 2: Filter out already-inserted artists
    remaining = unique_artists[~unique_artists['artist'].isin(existing_artists)]
    print(f"🎯 Processing {len(remaining)} new artists (skipped {len(unique_artists) - len(remaining)})")

    # Step 3: Loop with progress
    new_genres = []
    progress_bar = st.progress(0.0)
    status_text = st.empty()

    for idx, row in remaining.iterrows():
        artist = row['artist']
        track_uri = row['spotify_track_uri']

        genre = get_artist_genre_from_track_uri(track_uri, access_token)

        if genre == 'rate_limited':
            st.warning(f"Rate limited on {artist}. Skipping.")
            continue

        new_genres.append({'artist': artist, 'genre': genre})

        try:
            supabase.table("artist_genres").insert({"artist": artist, "genre": genre}).execute()
        except Exception as e:
            print(f"Supabase insert failed for {artist}: {e}")

        status_text.info(f"🎵 {len(new_genres)}/{len(remaining)}: {artist} → {genre}")
        progress_bar.progress(min(len(new_genres) / len(remaining), 1.0))
        time.sleep(0.1)

    progress_bar.empty()
    status_text.success(f"🎉 Done! Genres fetched for {len(new_genres)} artists.")

    return pd.DataFrame(new_genres)



st.markdown("---")
st.subheader("Get Spotify Genres for Artists")
st.markdown("""
### Genre Collection:
- The only data this website will collect and house is your artist and corresponding genre, but nothing else. This will build a database to decrease the amount of API calls needed for future users.
- So, the stored data will only look like 'Pixies - Alt Rock' - no information about you
            """)
if 'access_token' in st.session_state and 'distinct_artists' in st.session_state:
    if st.button("🎧 Get Genres and Join with Music Data"):
        with st.spinner("Calling Spotify API and merging..."):

            # STEP 1: Call the API
            artist_genres_df = get_genres_for_unique_artists(
                music_df=st.session_state['distinct_artists'],
                access_token=st.session_state['access_token']
            )

            # STEP 2: Store artist genres in session for re-use
            st.session_state['artist_genres_df'] = artist_genres_df

            # STEP 3: Rename and prepare full music dataset
            music_df = st.session_state['music_ts'].copy()
            column_renames = {
                'master_metadata_track_name': 'track_name',
                'master_metadata_album_artist_name': 'artist',
                'master_metadata_album_album_name': 'album_name',
            }
            music_df.rename(columns=column_renames, inplace=True)

            # STEP 4: Merge genres with music data
            music_df_with_genres = music_df.merge(artist_genres_df, on='artist', how='left')

            # STEP 5: Clean up columns and format
            columns_to_keep = [
                'ts',
                'ms_played',
                'track_name',
                'artist',
                'album_name',
                'reason_start',
                'reason_end',
                'shuffle',
                'skipped',
                'offline',
                'genre',
                'incognito_mode'
            ]
            music_df_with_genres['ts'] = pd.to_datetime(music_df_with_genres['ts'], utc=True)
            final_data = music_df_with_genres[columns_to_keep].copy()

            # STEP 6: Save to session + show
            st.session_state['final_data'] = final_data
            st.success("Genres joined successfully!")
            st.dataframe(final_data.head())

####################################################################################################################################################
'''

Feature engineering 

'''

if 'final_data' in st.session_state:
    final_data = st.session_state['final_data'].copy()

    # ---- Feature 1: Skip Variable ----
    final_data['skip'] = final_data['ms_played'].apply(lambda x: int(x <= 30000))

    # ---- Feature 2: Time of Day ----
    def get_time_of_day(ts):
        hour = ts.hour
        if 6 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 17:
            return 'afternoon'
        elif 17 <= hour < 21:
            return 'evening'
        else:
            return 'night'

    final_data['time_of_day'] = final_data['ts'].apply(get_time_of_day)

    # ---- Save & Show ----
    st.session_state['final_data'] = final_data
    st.success("Feature engineering complete!")
    st.dataframe(final_data.head())


###################################################################################################################################################


if 'final_data' in st.session_state:
    final_data = st.session_state['final_data'].copy()

    # Step 1: Feature selection and one-hot encoding
    X_raw = final_data[['reason_start', 'shuffle', 'incognito_mode', 'genre', 'time_of_day']]
    y = final_data['skip']
    X_encoded = pd.get_dummies(X_raw, drop_first=True)

    # Step 2: Train model
    X_train, X_test, y_train, y_test = train_test_split(X_encoded, y, test_size=0.25, random_state=1)
    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)

    # Step 3: Save to session
    st.session_state['model'] = model
    st.session_state['X_raw'] = X_raw
    st.session_state['X_encoded'] = X_encoded
    st.session_state['y'] = y

    # Step 4: Evaluate
    y_pred = model.predict(X_test)
    st.markdown("### Confusion Matrix")
    cm = confusion_matrix(y_test, y_pred)
    st.dataframe(pd.DataFrame(cm, columns=["Predicted 0", "Predicted 1"], index=["Actual 0", "Actual 1"]))

    st.markdown("### Classification Report")
    report = classification_report(y_test, y_pred, output_dict=True)
    st.dataframe(pd.DataFrame(report).transpose())

    # Step 5: Show log-odds
    coef_df = pd.DataFrame({
        "Feature": X_encoded.columns,
        "Log-Odds Coefficient": model.coef_[0]
    }).sort_values(by="Log-Odds Coefficient", ascending=False)
    st.session_state['coef_df'] = coef_df

    with st.expander("🔍 Log-Odds Coefficients"):
        st.dataframe(coef_df)

###################### prediction section ###########################################

st.header("🎶 Try Predicting a Skip")

# Pull from training data
if 'X_raw' in st.session_state and 'model' in st.session_state:
    base = st.session_state['X_raw']
    model = st.session_state['model']

    # UI widgets from column options
    genres = sorted(base['genre'].dropna().unique().tolist())
    reason_start_options = sorted(base['reason_start'].dropna().unique().tolist())
    shuffle_options = [False, True]
    incognito_options = [False, True]
    time_options = sorted(base['time_of_day'].dropna().unique().tolist())

    # Collect input
    selected_genre = st.selectbox("Select Genre", genres)
    selected_reason_start = st.selectbox("Reason for Start", reason_start_options)
    selected_shuffle = st.radio("Shuffle Enabled?", shuffle_options)
    selected_incognito = st.radio("Incognito Mode?", incognito_options)
    selected_time = st.selectbox("Time of Day", time_options)

    if st.button("Predict Skip?"):
        user_input = pd.DataFrame([{
            "reason_start": selected_reason_start,
            "shuffle": selected_shuffle,
            "incognito_mode": selected_incognito,
            "genre": selected_genre,
            "time_of_day": selected_time
        }])

        combined = pd.concat([base, user_input], axis=0)
        combined_encoded = pd.get_dummies(combined, drop_first=True)
        user_encoded = combined_encoded.tail(1)

        pred = model.predict(user_encoded)[0]
        st.markdown(f"### Will this song be skipped? **{'⏭️ Yes!' if pred == 1 else '🎶 No!'}**")

        
