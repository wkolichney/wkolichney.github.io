
import streamlit as st
import pandas as pd
import pickle
import requests
import io


st.markdown("""
## Spotify Skip Prediction Demo

This app uses a **logistic regression model** to predict whether a listener will skip a song based on playback behavior and genre. 

### How It Works
Using real Spotify listening data from a friend (Brian), I trained a machine learning model on features like:
- The reason the song started: Did the user open the app? Did they skip the previous song? Did they replay it? Did they select it?
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
    st.markdown(f"### Will the song be skipped? **{'⏭️Yes!' if pred == 1 else '🎶No!'}**")

    # 7. Optional: Show feature importances
    with st.expander("🔍 See What the Model Prioritizes (Log-Odds Coefficients)"):
        coef_df = pd.DataFrame({
            "Feature": model.feature_names_in_,
            "Log-Odds Coefficient": model.coef_[0]
        }).sort_values(by="Log-Odds Coefficient", ascending=False)

        st.dataframe(coef_df)


st.markdown("""
#### What are Log-Odds Coefficients?

The best part about logistic regression, when compared to other machine learning models, is that it's easier to interpret what features contribute to a skipped song.

We use Log-Odds coefficients. When training a logistic regression, what we are doing is creating an equation that uses our predictors (genre/time of day...) to solve for the song being skipped or not.

If we made the different genres, times of day, reasons for starting a song, etc., into variables of an equation, then the log-odds will tilt those variables to make the equation predict a skipped or a listened song. 

We let the model see the answer sometimes, called training data. That helps the model build the coefficients. 

Then we see if those coefficients were pretty accurate in cases the model hasn't seen before, called test data. It's not right all the time, no model is. 

#### How to Easily Interpret Log-Odds?
- **Positive values** (e.g., +1.5) mean the feature **increases** the likelihood of a skip. A positive number would lean the equation closer to "1" or "skip"
- **Negative values** (e.g., -2.0) mean the feature **reduces** the likelihood of a skip. A negative number would lean the equation closer to "0" or "not skipped"
- **Values close to 0** (e.g., 0.1 or -0.1) have **little or no effect** on the prediction. It doesn't drive the equation to 1 or 0.

As we can see in Brian's data, him skipping the previous song, or "reason_start_fwdbtn", usually means he will skip the song in question (this has a 0.8 coefficient). Meanwhile, if the genre is Alt-country, he'll probably listen to the song (-4.13 coefficient).



#### Is the model any good?
- **Overall accuracy:** 77%
- **Precision (skip = 1):** 74% — When the model predicts a skip, it's right 74% of the time.
- **Recall (skip = 1):** 70% — Of all the actual skips, the model catches 70% of them.
- **F1-Score (skip = 1):** 72% — A balance between precision and recall.

""")
