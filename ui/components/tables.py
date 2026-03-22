"""Table components with pagination for the election dashboard."""

import streamlit as st
import pandas as pd


@st.cache_data(show_spinner=False)
def _split_frame(df: pd.DataFrame, rows: int) -> list[pd.DataFrame]:
    return [df.loc[i: i + rows - 1, :] for i in range(0, len(df), rows)]


def paginate_table(table_data: pd.DataFrame) -> None:
    """Render a sortable, paginated dataframe."""
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=True, index=1)
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio("Direction", options=["⬆️", "⬇️"], horizontal=True)
        table_data = table_data.sort_values(
            by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True
        )

    bottom_menu = st.columns((4, 1, 1))
    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = max(1, int(len(table_data) / batch_size))
        current_page = st.number_input("Page", min_value=1, max_value=total_pages, step=1)
    with bottom_menu[0]:
        st.markdown(f"Page **{current_page}** of **{total_pages}**")

    pages = _split_frame(table_data, batch_size)
    st.container().dataframe(data=pages[current_page - 1], use_container_width=True)
