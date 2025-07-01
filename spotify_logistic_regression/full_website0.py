
'''

File Upload section

'''

import streamlit as st
import pandas as pd
import json
import requests
import base64
import urllib.parse

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
            st.success("Access token obtained successfully!")
            st.code(access_token)
        else:
            st.error(f"Failed to get access token: {response.status_code} - {response.text}")