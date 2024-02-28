import random

import psycopg2
import requests
import simplejson as json
from confluent_kafka import SerializingProducer

BASE_URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]
random.seed(42)

# Kafka Topics
voters_topic = 'voters_topic'
candidates_topic = 'candidates_topic'


def create_tables(conn, cur):
    """This function creates three tables in the PostgreSQL database if they do not already exist. 
    - The tables are candidates, voters, and votes. Each table has a specific schema defined in the SQL commands. 
    - The conn.commit() at the end ensures that these changes are saved to the database."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            biography TEXT,
            campaign_platform TEXT,
            photo_url TEXT
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)

    conn.commit()


def generate_voter_data():
    """This one generates data for a voter. It makes a GET request to the randomuser.me API and formats the response into a dictionary with keys like voter_id, voter_name, date_of_birth, etc."""
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    else:
        return "Error fetching data"


def generate_candidate_data(candidate_number, total_parties):
    """This function generates data for a candidate. It makes a GET request to the randomuser.me API and formats the response into a dictionary with keys like candidate_id, candidate_name, party_affiliation, etc. The party affiliation is chosen from a predefined list of parties, and the gender alternates based on the candidate number."""
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    if response.status_code == 200:
        user_data = response.json()['results'][0]

        return {
            "candidate_id": user_data['login']['uuid'],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": PARTIES[candidate_number % total_parties],
            "biography": "A brief bio of the candidate.",
            "campaign_platform": "Key campaign promises or platform.",
            "photo_url": user_data['picture']['large']
        }
    else:
        return "Error fetching data"


def insert_voters(conn, cur, voter):
    """This function inserts a voter into the voters table in the PostgreSQL database. It uses the psycopg2 library to execute an INSERT SQL command. If the voter already exists in the database, it catches the psycopg2.IntegrityError and prints a message. Otherwise, it commits the changes to the database. 
    - The voter parameter is a dictionary that contains the voter's data. 
    - The SQL command uses placeholders (%s) for the values, which are then provided as a tuple in the second argument to cur.execute(). 
    - The conn.commit() at the end ensures that the changes are saved to the database.""" 
    try:
        cur.execute("""
                    INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
                    """,
                    (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                    voter['nationality'], voter['registration_number'], voter['address']['street'],
                    voter['address']['city'], voter['address']['state'], voter['address']['country'],
                    voter['address']['postcode'], voter['email'], voter['phone_number'],
                    voter['cell_number'], voter['picture'], voter['registered_age'])
                    )
        conn.commit()
    except psycopg2.IntegrityError:
        print(f"Voter {voter['voter_id']} already exists in the database")
        conn.rollback()
            
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092',})
    create_tables(conn, cur)

    # get candidates from db
    cur.execute("""
        SELECT * FROM candidates
    """)
    candidates = cur.fetchall()
    print(candidates)

    if len(candidates) == 0:
        for i in range(3):
            candidate = generate_candidate_data(i, 3)
            print(candidate)
            cur.execute("""
                        INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'], candidate['biography'],
                candidate['campaign_platform'], candidate['photo_url']))
            conn.commit()

    for i in range(500):
        voter_data = generate_voter_data()
        insert_voters(conn, cur, voter_data)

        producer.produce(
            voters_topic,
            key=voter_data["voter_id"],
            value=json.dumps(voter_data),
            on_delivery=delivery_report
        )

        print(f'Produced voter {i}, data: {voter_data}')
        producer.flush() # flushes the producer to ensure all messages are delivered
