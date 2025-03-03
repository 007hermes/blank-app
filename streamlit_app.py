import streamlit as st
import pandas as pd
import plotly.express as px

# Load data (replace with your data loading logic)
@st.cache_data
def load_data():
    return pd.DataFrame({
        "DATE_GROUP": ["Nov-2024", "Dec-2024", "Jan-2025", "Feb-2025"],
        "TALKSCORE_OVERALL": [6.5, 7.0, 7.2, 7.5]
    })

df = load_data()

# Title
st.title("Talkscore Dashboard")

# Charts
st.header("Average Talkscore Overall Over Time")
fig1 = px.bar(df, x="DATE_GROUP", y="TALKSCORE_OVERALL")
st.plotly_chart(fig1)

# Add more charts and tables as needed
