
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


st.title("Upload Your Spotify JSON Files")

# Upload multiple JSON files
uploaded_files = st.file_uploader(
    "Choose one or more Spotify JSON files", 
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
        st.success("All files loaded and combined!")
        st.dataframe(combined_df)


# SECTION 2: Spotify Developer Credentials
st.markdown("---")
st.header("Spotify Developer Credentials")
client_id = st.text_input("Client ID")
client_secret = st.text_input("Client Secret", type="password")
redirect_uri = "https://wkolichneyappio-spotify-machine-learning.streamlit.app/"
scope = "user-library-read user-top-read"

# STEP 1: Generate Authorization URL
if client_id and client_secret:
    st.markdown("### Step 1: Authorize Application")
    auth_url = (
        f"https://accounts.spotify.com/authorize"
        f"?client_id={client_id}"
        f"&response_type=code"
        f"&redirect_uri={urllib.parse.quote(redirect_uri)}"
        f"&scope={urllib.parse.quote(scope)}"
    )
    st.markdown(f"[Click here to authorize Spotify access]({auth_url})")
    auth_code = st.text_input("Step 2: Paste the 'code' from the URL after authorization")

    # STEP 3: Exchange Code for Access Token
    if auth_code:
        st.markdown("### Step 3: Get Access Token")
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
'''
That's pretty much is user side. Now, I need to cleanup their json data, remove personal information, make api call, 
store genre data in SQL or...something


'''

# Ask the user for their timezone
st.markdown("---")
st.subheader("Choose Your Timezone")
timezone_options = pytz.all_timezones
user_timezone = st.selectbox("What region do you live in? (Used to convert timestamps)", options=timezone_options, index=timezone_options.index("America/New_York"))

if df is not None and not df.empty:
    # STEP 1: Select and rename relevant columns
    music = df[['ts', 'ms_played', 'master_metadata_track_name', 
                'master_metadata_album_artist_name', 'master_metadata_album_album_name',
                'reason_start', 'reason_end', 'shuffle', 'skipped', 'offline', 
                'spotify_track_uri']].copy()

    music = music.rename(columns={
        'master_metadata_track_name': 'track',
        'master_metadata_album_artist_name': 'artist',
        'master_metadata_album_album_name': 'album'
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

    # STEP 4: Prepare distinct track list
    more_than_genres_api = music_ts.drop_duplicates(subset=['track'], keep='first')[['track', 'artist', 'spotify_track_uri']]

    st.markdown("### ✅ Processed Artist & Track Tables")
    st.write("Distinct Artists")
    st.dataframe(distinct_artist_try.head())

    st.write("Distinct Tracks")
    st.dataframe(more_than_genres_api.head())

    # Optional: Save to session state if needed later
    st.session_state['music_ts'] = music_ts
    st.session_state['distinct_artists'] = distinct_artist_try
