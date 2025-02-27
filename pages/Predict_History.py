import streamlit as st
import pandas as pd
from configFiles.config import API_URL
import requests

def show():
    st.title("📊 Past Predictions")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("📅 Start Date")
    with col2:
        end_date = st.date_input("📅 End Date")

    source_filter = st.selectbox(
        "🔍 Select Prediction Source",
        ["All", "WebApp", "Scheduled Predictions"]
    )

    if st.button("🔄 Fetch Predictions"):
        response = requests.get(f"{API_URL}/past-predictions", params={
            "start_date": start_date,
            "end_date": end_date,
            "source": source_filter.lower()
        })

        if response.status_code == 200:
            data = response.json()

            # ✅ Check if data is empty before converting to DataFrame
            if not data or len(data) == 0:
                st.warning("⚠️ No prediction history found for the selected filters.")
                return
            
            df = pd.DataFrame(data)
            
            # ✅ Ensure the response contains an "id" column before setting it as index
            if "id" in df.columns:
                df = df.set_index("id")

            st.subheader("✅ Prediction Results")
            st.dataframe(df, use_container_width=True)
        else:
            st.error(f"❌ Failed to fetch data: {response.status_code}")

show()