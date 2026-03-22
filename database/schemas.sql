-- Database Schema for Realtime Voting Application
--
-- This file contains all DDL statements for reference and manual operations.
-- The application uses repositories.py for table creation.

-- Candidates table: stores election candidate information
CREATE TABLE IF NOT EXISTS candidates (
    candidate_id VARCHAR(255) PRIMARY KEY,
    candidate_name VARCHAR(255) NOT NULL,
    party_affiliation VARCHAR(255) NOT NULL,
    biography TEXT,
    campaign_platform TEXT,
    photo_url TEXT
);

-- Index for party-based queries
CREATE INDEX IF NOT EXISTS idx_candidates_party ON candidates(party_affiliation);

-- Voters table: stores registered voter information
-- Address is flattened (denormalized) for query simplicity
CREATE TABLE IF NOT EXISTS voters (
    voter_id VARCHAR(255) PRIMARY KEY,
    voter_name VARCHAR(255) NOT NULL,
    date_of_birth VARCHAR(255),
    gender VARCHAR(255),
    nationality VARCHAR(255),
    registration_number VARCHAR(255) UNIQUE,
    address_street VARCHAR(255),
    address_city VARCHAR(255),
    address_state VARCHAR(255),
    address_country VARCHAR(255),
    address_postcode VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(255),
    cell_number VARCHAR(255),
    picture TEXT,
    registered_age INTEGER CHECK (registered_age >= 0)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_voters_state ON voters(address_state);
CREATE INDEX IF NOT EXISTS idx_voters_gender ON voters(gender);

-- Votes table: records cast ballots
-- voter_id UNIQUE ensures one vote per voter
CREATE TABLE IF NOT EXISTS votes (
    voter_id VARCHAR(255) UNIQUE,
    candidate_id VARCHAR(255),
    voting_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    vote INT DEFAULT 1 CHECK (vote = 1),
    PRIMARY KEY (voter_id, candidate_id)
);

-- Index for time-based queries and aggregation
CREATE INDEX IF NOT EXISTS idx_votes_time ON votes(voting_time);
CREATE INDEX IF NOT EXISTS idx_votes_candidate ON votes(candidate_id);
