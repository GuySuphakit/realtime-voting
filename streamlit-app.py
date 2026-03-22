import time

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from config import settings
from database.repositories import CandidateRepository, VoterRepository
from kafka_utils.consumer import StreamlitKafkaConsumer
from ui.components.charts import plot_bar_chart, plot_donut_chart
from ui.components.tables import paginate_table


@st.cache_data(ttl=30)
def fetch_voting_stats() -> tuple[int, int]:
    """Return (voters_count, candidates_count) from the database."""
    return VoterRepository().count(), CandidateRepository().count()


def fetch_kafka_data(topic: str) -> pd.DataFrame:
    consumer = StreamlitKafkaConsumer(topic)
    data = consumer.fetch_all(timeout_ms=1000)
    consumer.close()
    return pd.DataFrame(data)


def update_data() -> None:
    st.empty().text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    voters_count, candidates_count = fetch_voting_stats()
    st.markdown("---")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)

    # --- Leading candidate ---
    results = fetch_kafka_data(settings.kafka.aggregated_votes_per_candidate_topic)
    if results.empty:
        st.info("No vote data available yet.")
        return

    results = results.loc[results.groupby("candidate_id")["total_votes"].idxmax()]
    leading = results.loc[results["total_votes"].idxmax()]

    st.markdown("---")
    st.header("Leading Candidate")
    col1, col2 = st.columns(2)
    with col1:
        try:
            st.image(leading["photo_url"], width=200)
        except Exception:
            st.caption("Photo unavailable")
    with col2:
        st.header(leading["candidate_name"])
        st.subheader(leading["party_affiliation"])
        st.subheader(f"Total Votes: {leading['total_votes']}")

    # --- Statistics ---
    st.markdown("---")
    st.header("Statistics")
    results = results[["candidate_id", "candidate_name", "party_affiliation", "total_votes"]]
    results = results.reset_index(drop=True)

    col1, col2 = st.columns(2)
    with col1:
        st.pyplot(plot_bar_chart(results))
    with col2:
        st.pyplot(plot_donut_chart(results))

    st.table(results)

    # --- Location turnout ---
    location_result = fetch_kafka_data(settings.kafka.aggregated_turnout_by_location_topic)
    location_result = location_result.loc[location_result.groupby("state")["count"].idxmax()]
    location_result = location_result.reset_index(drop=True)

    st.header("Location of Voters")
    paginate_table(location_result)

    st.session_state["last_update"] = time.time()


def sidebar() -> None:
    if st.session_state.get("last_update") is None:
        st.session_state["last_update"] = time.time()

    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    if st.sidebar.button("Refresh Data"):
        update_data()


st.title("Realtime Election Dashboard")
sidebar()
update_data()
