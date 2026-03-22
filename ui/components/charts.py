"""Chart components for the election dashboard."""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_bar_chart(results: pd.DataFrame) -> plt.Figure:
    """Colored bar chart of vote counts per candidate."""
    colors = plt.cm.viridis(np.linspace(0, 1, len(results)))
    fig, ax = plt.subplots()
    ax.bar(results["candidate_name"], results["total_votes"], color=colors)
    ax.set_xlabel("Candidate")
    ax.set_ylabel("Total Votes")
    ax.set_title("Vote Counts per Candidate")
    plt.xticks(rotation=90)
    return fig


def plot_donut_chart(data: pd.DataFrame, title: str = "Vote Distribution") -> plt.Figure:
    """Donut chart of vote distribution by candidate."""
    fig, ax = plt.subplots()
    ax.pie(
        list(data["total_votes"]),
        labels=list(data["candidate_name"]),
        autopct="%1.1f%%",
        startangle=140,
    )
    ax.axis("equal")
    ax.set_title(title)
    return fig
