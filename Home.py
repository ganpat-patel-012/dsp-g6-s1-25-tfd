import streamlit as st

st.set_page_config(page_title="The Flight Detectives", layout="wide")

st.markdown("""<h1 style='text-align: center; color: #FF5733;'>🕵️‍♂️ The Flight Detectives 🕵️‍♀️</h1>""", unsafe_allow_html=True)

st.markdown("""<h3 style='text-align: center; color: #4CAF50;'>Cracking the mystery of unpredictable flight prices... one fare at a time! ✈️💰</h3>""", unsafe_allow_html=True)

st.markdown("""
Welcome, 🕵️‍♂️ Have you ever wondered why flight prices change faster than your mood on a Monday morning? Well, worry no more! 
We're here to investigate, analyze, and predict flight prices with our top-secret, highly classified, (not really) machine learning model. 

Use the sidebar to make predictions, check past cases (a.k.a. previous predictions), and uncover the secrets of airfare fluctuations!

💡 **Disclaimer:** We take no responsibility if you end up spending all your savings on spontaneous getaways!
""", unsafe_allow_html=True)

st.markdown("""<h2 style='text-align: center; color: #3498DB;'>Meet The Detectives</h2>""", unsafe_allow_html=True)

team_members = [
    {"name": "Ganpat Patel", "title": "StreamLit, ML Model, FastAPI, PostgreSQL & Docker", "image": "images/gp.jpg"},
    {"name": "JatinKumar Parmar", "title": "Data Preparation, Data Ingestion & Validation", "image": "images/jp.jpg"},
    {"name": "Adnan Ali", "title": "Airflow Data Ingestion Job", "image": "images/aa.jpg"},
    {"name": "Musa Ummar", "title": "Airflow Prediction Job Scheduling", "image": "images/mu.jpg"},
    {"name": "Manoj Kumar", "title": "Grafana And PostgreSQL", "image": "images/mn.jpeg"},
]

cols = st.columns(5)
for idx, member in enumerate(team_members):
    with cols[idx]:
        st.image(member["image"], width=150)
        st.markdown(f"**{member['name']}**")
        st.caption(member["title"])

st.markdown("<hr>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: grey;'>Made with ❤️ and a whole lot of coffee ☕ by The Flight Detectives</p>", unsafe_allow_html=True)

#run - streamlit run Home.py 