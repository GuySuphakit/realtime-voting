# CLAUDE.md

## Project Overview

Real-time election voting system using Python, Kafka, Spark Streaming, PostgreSQL, and Streamlit. Docker Compose orchestrates all infrastructure services.

## Architecture

```
main.py          → setup: DB tables, Kafka topics, generate candidates/voters
voting.py        → simulate votes: consume voters_topic → produce votes_topic
spark-streaming.py → aggregate votes via Spark → publish to aggregated_* topics
streamlit-app.py → live dashboard: reads from Kafka + PostgreSQL
```

Infrastructure (via Docker Compose): Zookeeper, Kafka broker (`localhost:9092`), PostgreSQL (`localhost:5432`), Spark master/worker.

## Package Structure

- `config/settings.py` — singleton settings loaded from `.env` (DB, Kafka, Spark, app config)
- `models/` — Pydantic v2 models: `Candidate`, `Voter`, `Vote` (includes PySpark schema on `Vote`)
- `services/` — business logic: `data_generator.py` (calls randomuser.me API), `voting_service.py`
- `kafka_utils/` — `KafkaProducerWrapper`, `KafkaConsumerWrapper`, serializers, exceptions
- `database/` — connection management, CRUD repositories, `schemas.sql`, exceptions

## Development Setup

```bash
# Install dependencies
uv sync

# Copy and configure environment
cp .env.example .env

# Start infrastructure
docker-compose up -d

# Run system (in separate terminals)
python main.py           # initialise
python voting.py         # simulate votes
python spark-streaming.py
streamlit run streamlit-app.py
```

## Code Style

- **Formatter**: `black` (line length 100, Python 3.11)
- **Linter**: `ruff` (replaces flake8/isort; rules: E, W, F, I, B, C4)
- Run both before committing:

```bash
uv run black .
uv run ruff check . --fix
```

## Testing

```bash
uv run pytest
```

Test file: `test_phase1.py`

## Key Config

- Python 3.11+
- JDBC driver for Spark ↔ PostgreSQL: `postgresql-42.7.2.jar` (must be present at repo root)
- Default DB credentials (local Docker): `postgres/postgres`, database `voting`
