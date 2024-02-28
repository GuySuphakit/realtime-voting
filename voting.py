import random
import time
from datetime import datetime
import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
from main import delivery_report

conf = {
    'bootstrap.servers': 'localhost:9092',
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)


def consume_messages():
    """This function subscribes to the 'candidates_topic' and attempts to consume messages from Kafka.
    - It collects and appends messages to the `result` list until it has received three messages.
    - The function returns the collected messages."""
    result = []
    consumer.subscribe(['candidates_topic'])
    try:
        while True:
            msg = consumer.poll(timeout=1.0) # timeout in seconds, If no message is received within this time, the poll method will return None and the loop will continue to the next iteration.
            if msg is None: 
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF: # end of partition (no more messages to consume)
                    continue
                else:
                    print(msg.error())
                    break
            else:
                result.append(json.loads(msg.value().decode('utf-8'))) # append message to result list
                if len(result) == 3:
                    return result

            # time.sleep(5)
    except KafkaException as e:
        print(e)


if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # candidates
    candidates_query = cur.execute("""
        SELECT row_to_json(t)
        FROM (
            SELECT * FROM candidates
        ) t;
    """)
    candidates = cur.fetchall() # fetch all rows from the result set
    candidates = [candidate[0] for candidate in candidates] # convert list of tuples to list of dictionaries
    if len(candidates) == 0:
        raise Exception("No candidates found in database")
    else:
        print(candidates)

    consumer.subscribe(['voters_topic'])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8')) # convert message to dictionary
                chosen_candidate = random.choice(candidates) 
                vote = voter | chosen_candidate | {
                    "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "vote": 1
                } # merge dictionaries 

                try:
                    print("User {} is voting for candidate: {}".format(vote['voter_id'], vote['candidate_id']))
                    cur.execute("""
                            INSERT INTO votes (voter_id, candidate_id, voting_time)
                            VALUES (%s, %s, %s)
                        """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))

                    conn.commit()

                    producer.produce(
                        'votes_topic',
                        key=vote["voter_id"],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )
                    producer.poll(0) # wait for message to be delivered
                except Exception as e:
                    print("Error: {}".format(e))
                    conn.rollback() # rollback the transaction
                    continue
            time.sleep(0.2)
    except KafkaException as e:
        print(e)
