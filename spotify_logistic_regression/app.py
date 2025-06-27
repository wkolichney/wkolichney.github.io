
import streamlit as st
import pandas as pd
import pickle
import requests
import io


st.markdown("""
## Spotify Skip Prediction Demo

This app uses a **logistic regression model** to predict whether a listener will skip a song based on playback behavior and genre. 

### How It Works
Using real Spotify listening data from a friend, I trained a machine learning model on features like:
- The reason the song started started: Did the user open the app? Did they skip the previous song? Did they replay it? Did they select it?
- Whether shuffle was on
- If the user was in incognito mode
- The genre of the song
- The time of day

These inputs are converted into one-hot encoded variables and passed to a logistic regression model trained with scikit-learn. The model returns whether a skip is likely, as a simple "Yes" or "No".

### Tech Stack
- **Python & pandas** for data wrangling
- **Scikit-learn** for model training
- **Streamlit** for the web app interface
- **GitHub Pages** for hosting the dataset and model pickle file

### Try It Yourself
Use the dropdowns and radio buttons to set the playback scenario. Then click “Predict” to see whether the model thinks the user will skip the track.

### Notes

Some variables are more important than others. Sometimes, it won't matter what time of day it is or a particular genre of music. Brian may skip the song if they skipped the previous song and this may overpower the other variables.

---
""")


# Load data
url = "https://wkolichney.github.io/Data/brian_final_data.csv"
df = pd.read_csv(url)
# load model
model_url = "https://wkolichney.github.io/Data/logistic_model.pkl"
response = requests.get(model_url)
model = pickle.load(io.BytesIO(response.content))
# Select genres that users can choose

genres = sorted(df['genre'].dropna().unique().tolist())
reason_start_options = sorted(df['reason_start'].dropna().unique().tolist())
reason_end_options = sorted(df['reason_end'].dropna().unique().tolist())
shuffle_options = [False, True]
incognito_options = [False, True]
time_options = sorted(df['time_of_day'].dropna().unique().tolist())

st.title("Spotify Skip Prediction Demo")

selected_genre = st.selectbox("Select Genre", genres)
selected_reason_start = st.selectbox("Reason for Start", reason_start_options)
selected_shuffle = st.radio("Shuffle Enabled?", shuffle_options)
selected_incognito = st.radio("Incognito Mode?", incognito_options)
selected_time = st.selectbox("Time of Day", time_options)



if st.button("Predict"):
    # 1. Capture user inputs
    user_input = pd.DataFrame([{
        "reason_start": selected_reason_start,
        "shuffle": selected_shuffle,
        "incognito_mode": selected_incognito,
        "genre": selected_genre,
        "time_of_day": selected_time
    }])

    # 2. Concatenate with training data columns to get consistent dummy variables
    base = df[["reason_start", "shuffle", "incognito_mode", "genre", "time_of_day"]]
    combined = pd.concat([base, user_input], axis=0)

    # 3. One-hot encode with same logic as model
    combined_encoded = pd.get_dummies(combined, drop_first=True)

    # 4. Take the last row (user input, now encoded properly)
    user_encoded = combined_encoded.tail(1)

    # 5. Predict
    pred = model.predict(user_encoded)[0]

    # 6. Display result
    st.markdown(f"### Will the song be skipped? **{'Yes' if pred == 1 else 'No'}**")
