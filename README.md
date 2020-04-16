# focus-sensors

Assumes a *nix based machine.

## LOCAL

To run the tests do the following:

```shell script
$> pipenv update
$> pipenv shell
$> python -m unittest tests/test_*.py
```

## CLOUD (GCP)


## To setup Kafka on a local machine

1. Download Kafka from https://kafka.apache.org/downloads.
1. Follow steps 1, 2 and 3 from https://kafka.apache.org/quickstart.  I.e. Untar kafka, start the server and create a Kafka topic.

## Running the code

1. Create a Python virtual environment: `python3 -m venv env`.
1. Activate the virtual environment: `source env/bin/activate`.
1. Install dependencies: `pip install -r requirements.txt`.
1. Set the variables as required at the top of the `producer.py` and `consumer.py` files.
1. Start the consumer: `python consumer.py`.
1. Start the producer: `python producer.py`.

## To cleanup Kafka topic

You need to do this if playing with the code many times as it will eat up disk space quickly!

1. `./bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic test`
1. Stop Kafka and Zookeeper then run `rm -rf /tmp/kafka-logs/`.
1. Start Kafka and Zookeeper then re-create the topic, e.g. 
   `bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test`.
   
## Documentation

The documentation for the Python Kafka API can be found here:

* https://github.com/confluentinc/confluent-kafka-python.

