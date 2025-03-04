import streamlit as st
# Import python packages
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, avg, count, when, to_date
import pandas as pd
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go

# Create a Snowflake session
def main(session: snowpark.Session): 
    # Load data from Snowflake
    query = """
    SELECT
            CAST(invitationdt_UTC AS DATE) AS DATE_DAY,
            CASE WHEN campaign_site = '' THEN 'Unknown' ELSE campaign_site END AS camp_site,
            talkscore_cefr,
            talkscore_vocab,
            talkscore_fluency,
            talkscore_grammar,
            talkscore_comprehension,
            talkscore_pronunciation,
            talkscore_overall,
            CASE WHEN talkscore_cefr <> '' THEN 1 ELSE 0 END AS  Test_Completed,
            CASE WHEN talkscore_cefr = '' THEN 1 ELSE 0 END AS Test_Uncompleted,
            CASE WHEN LABELS LIKE '%For TS Review%' THEN 1 ELSE 0 END AS For_TS_Review

    FROM (
            SELECT
                invitationdt AS invitationdt_UTC,
                campaign_site,
                LABELS,
        PARSE_JSON(otherinformation) AS json,
        IFNULL(json:talkscore_cefr::VARCHAR,'') AS talkscore_cefr,
        IFNULL(TRY_TO_DECIMAL(json:talkscore_vocab::VARCHAR, 5, 2), 0) AS talkscore_vocab,
        IFNULL(TRY_TO_DECIMAL(json:talkscore_fluency::VARCHAR, 5, 2), 0) AS talkscore_fluency,
        IFNULL(TRY_TO_DECIMAL(json:talkscore_grammar::VARCHAR, 5, 2), 0) AS talkscore_grammar,
        IFNULL(TRY_TO_DECIMAL(json:talkscore_comprehension::VARCHAR, 5, 2), 0) AS talkscore_comprehension,
        IFNULL(TRY_TO_DECIMAL(json:talkscore_pronunciation::VARCHAR, 5, 2), 0) AS talkscore_pronunciation,
        IFNULL(TRY_TO_DECIMAL(json:talkscore_overall::VARCHAR, 5, 2), 0) AS talkscore_overall
            FROM talkpush.dbo.tbltalkpushcandidateinfo
            WHERE crminstance LIKE '%dava%' AND invitationdt >= '2024-03-01' AND invitationdt < '2025-03-01'
    ) a;
    """
    # Fetch data using collect() and convert to pandas DataFrame
    rows = session.sql(query).collect()
    df = pd.DataFrame(rows, columns=["DATE_DAY", "CAMP_SITE", "TALKSCORE_CEFR", "TALKSCORE_VOCAB", "TALKSCORE_FLUENCY", "TALKSCORE_GRAMMAR", 	
                                     "TALKSCORE_COMPREHENSION", "TALKSCORE_PRONUNCIATION", 	"TALKSCORE_OVERALL", "TEST_COMPLETED", "TEST_UNCOMPLETED", "FOR_TS_REVIEW"])
    
     # Convert DATE_DAY to datetime
    df["DATE_DAY"] = pd.to_datetime(df["DATE_DAY"])

    # Apply Aggregation based on Selection
    #monthly option
    df["MONTHLY_"] = df["DATE_DAY"].dt.strftime("%b-%Y")  # Format as Feb-2024
    #weekly option
    df["WEEKLY_"] = "W_" + (df["DATE_DAY"] + pd.to_timedelta(6 - df["DATE_DAY"].dt.weekday, unit="D")).dt.strftime("%b-%d-%Y")

    
    # Filter out rows where talkscore_overall is 0
    df_filtered = df[df["TALKSCORE_OVERALL"] > 0]
    # FIG1 and FIG1w Aggregate Data
    df_avg_overall = df_filtered.groupby("MONTHLY_", as_index=False)["TALKSCORE_OVERALL"].mean()
    df_avg_overall_w = df_filtered.groupby("WEEKLY_", as_index=False)["TALKSCORE_OVERALL"].mean()
        # Format the values explicitly to 2 decimal places
    df_avg_overall["TEXT_LABEL"] = df_avg_overall["TALKSCORE_OVERALL"].apply(lambda x: f"{x:.2f}")
    df_avg_overall_w["TEXT_LABEL"] = df_avg_overall_w["TALKSCORE_OVERALL"].apply(lambda x: f"{x:.2f}")

    # Convert MONTHLY_ to datetime for proper sorting
    df_avg_overall['MONTHLY_'] = pd.to_datetime(df_avg_overall['MONTHLY_'], format='%b-%Y')
    df_avg_overall_w['SORT_KEY'] = pd.to_datetime(df_avg_overall_w['WEEKLY_'].str[2:], format='%b-%d-%Y') 

    # Sort the DataFrame by MONTHLY_
    df_avg_overall = df_avg_overall.sort_values('MONTHLY_')
    df_avg_overall_w = df_avg_overall_w.sort_values('SORT_KEY')


    # FIG 1: Clustered Column (Talkscore Overall)
    fig1 = px.line(df_avg_overall, 
               x="MONTHLY_", 
               y="TALKSCORE_OVERALL",
               markers=True,  # Add points (vertices)
               title="Average Talkscore Overall Over Time",
               labels={"MONTHLY_": "Time", "TALKSCORE_OVERALL": "Avg Talkscore"},
               line_shape="linear",
               text="TEXT_LABEL")  # Use formatted text
        # Update the trace to display the text on the chart
    fig1.update_traces(textposition="top center")
      
    # FIG 1 w: Clustered Column (Talkscore Overall) WEEKLY
    fig1w = px.line(df_avg_overall_w, 
                x="WEEKLY_", y="TALKSCORE_OVERALL",
                markers=True,  # Add points (vertices)
                title="Average Talkscore Overall Over Time",
                labels={"WEEKLY_": "Time", "TALKSCORE_OVERALL": "Avg Talkscore"},
                line_shape="linear",text="TEXT_LABEL")
        # Update the trace to display the text on the chart
    fig1w.update_traces(textposition="top center")
    # Input widgets
    col = st.columns(2)
    # Display Charts
    with col[0]:st.plotly_chart(fig1)
    with col[1]:st.plotly_chart(fig1w)