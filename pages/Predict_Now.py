import streamlit as st
import pandas as pd
from configFiles.makePrediction import get_prediction, get_batch_prediction
from configFiles.dbCode import insert_prediction
from datetime import datetime

def show():
    st.title("✈️ Flight Price Prediction")
    tab1, tab2 = st.tabs(["📌 Single Prediction", "📂 Multi Prediction"])

    with tab1:
        st.subheader("🔍 Single Prediction")
        col1, col2 = st.columns(2)
        with col1:
            airline = st.selectbox("✈️ Airline", ["AirAsia", "Air_India", "GO_FIRST", "Indigo", "SpiceJet", "Vistara"])
            source_city = st.selectbox("📍 Source City", ["Bangalore", "Chennai", "Delhi", "Hyderabad", "Kolkata", "Mumbai"])
            departure_time = st.selectbox("🛫 Departure Time", ["Early_Morning", "Morning", "Afternoon", "Evening", "Night", "Late_Night"])
            travel_class = st.selectbox("💺 Class", ["Business", "Economy"])
            stops = st.selectbox("⏳ Total Stops", ["zero", "one", "two_or_more"])
        with col2:
            destination_city = st.selectbox("📍 Destination City", ["Chennai", "Bangalore", "Delhi", "Hyderabad", "Kolkata", "Mumbai"])
            arrival_time = st.selectbox("🛬 Arrival Time", ["Early_Morning", "Morning", "Afternoon", "Evening", "Night", "Late_Night"])
            duration = st.number_input("⏱️ Duration (hours)", min_value=0.5, step=0.5)
            days_left = st.number_input("📆 Days Left", min_value=0, step=1)

        if source_city == destination_city:
            st.error("❌ Source and Destination cities cannot be the same!")
        else:
            if st.button("🚀 Predict"):
                payload = {
                    "airline": airline, "source_city": source_city,
                    "destination_city": destination_city, "departure_time": departure_time,
                    "arrival_time": arrival_time, "travel_class": travel_class,
                    "stops": stops, "duration": duration, "days_left": days_left
                }
                predicted_price = get_prediction(payload)
                st.write("Predicted Price (₹)", predicted_price)

                result_data = {**payload, "predicted_price": predicted_price, "prediction_source": "WebApp", "prediction_type": "Single"}
                msg = insert_prediction(result_data)
                st.success(msg)
                st.dataframe(pd.DataFrame([result_data]), use_container_width=True)

    with tab2:
        st.subheader("📂 Upload CSV for Multi-Prediction")
        uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

        if uploaded_file is not None:
            df = pd.read_csv(uploaded_file)
            st.subheader("📋 Uploaded Data Preview")
            st.dataframe(df, use_container_width=True)

            required_columns = ["airline", "source_city", "destination_city", "departure_time", "arrival_time", 
                                "travel_class", "stops", "duration", "days_left"]
            missing_cols = [col for col in required_columns if col not in df.columns]

            if missing_cols:
                st.error(f"❌ Missing columns: {', '.join(missing_cols)}")
            else:
                df_filtered = df[df['source_city'] != df['destination_city']]
                if df_filtered.shape[0] != df.shape[0]:
                    st.warning("⚠️ Some rows were removed due to same source & destination.")

                if st.button("🚀 Predict for CSV"):
                    payload_list = df_filtered.to_dict(orient="records")
                    predictions = get_batch_prediction(payload_list)
                    
                    # Add predicted price and metadata to each record
                    for i, pred in enumerate(predictions):
                        payload_list[i]["predicted_price"] = pred["predicted_price"]
                        payload_list[i]["prediction_source"] = "WebApp"
                        payload_list[i]["prediction_type"] = "Multiple"
                        payload_list[i]["prediction_time"] = datetime.now()

                    # Perform batch insert
                    msg = insert_prediction(payload_list)
                    st.success(f"✅ Batch {len(payload_list)} predictions saved successfully. {msg}")
                    st.dataframe(pd.DataFrame(payload_list), use_container_width=True)

show()