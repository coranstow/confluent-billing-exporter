#!/usr/bin/env python
from requests.exceptions import HTTPError
import json
import time
from argparse import ArgumentParser, FileType
from configparser import ConfigParser

import configargparse
import requests
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer


def key_billing_data(data):
    key = {'id': data['resource']['id'],
           'start_date': data['start_date'],
           'granularity': data['granularity']}
    return json.dumps(key)


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for Billing record {}: {}".format(msg.key(), err))
        return
    # print('Billing record {} successfully produced to {} [{}] at offset {}'.format(
    #     msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main():
    # Metrics
    start_time = time.time()
    api_request_counter = 0
    data_row_counter = 0
    produce_to_kafka_counter = 0

    p = configargparse.ArgParser(description='Confluent Billing Importer Configuration',
                                 default_config_files=['client.properties'])
    p.add_argument('--bootstrap.server', required=True, env_var='CONFLUENT_BOOTSTRAP',
                   help='The bootstrap string of the cluster to write to')



    # Get the config file from the args .
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Get the schems we'll use for the billing line items
    with open("billing_data_schema.json") as f:
        schema_str = f.read()

    schema_registry_conf = dict(config_parser['schema_registry'])
    # Have to rename the url from the config generated by Confluent Cloud
    schema_registry_conf['url'] = schema_registry_conf['schema.registry.url']
    schema_registry_conf.pop('schema.registry.url')
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # Set the key and value serialisers
    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client)

    # Create Producer instance
    producer = Producer(config)

    # Set the outbound topic from the value in the config file
    topic = config_parser.get('producer', 'topic')

    # Create the initial request to the REST API. Subsequent requests will use the pagination feature
    api_url = config_parser.get('rest_api', 'url')
    auth = config_parser.get('rest_api', 'rest.auth.username'), config_parser.get('rest_api', 'rest.auth.password')

    response = requests.get(api_url, params=dict(config_parser['request']), auth=auth)
    api_request_counter += 1

    next_url = 'This is not empty, so we will run at least once'
    backoff_time = 0

    while next_url:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            if response.status_code == 200:
                backoff_time = 0
                billing_response = response.json()
                billing_data = billing_response['data']
                next_url = billing_response['metadata']['next']
                data_row_counter += len(billing_data)

                # Iterate billing_data and produce to Kafka
                for data in billing_data:
                    key = key_billing_data(data)
                    producer.produce(topic=topic,
                                     key=string_serializer(key),
                                     value=json_serializer(data, SerializationContext(topic, MessageField.VALUE)),
                                     on_delivery=delivery_report)
            elif response.status_code == 429:
                backoff_time += 1
                print(
                    f'She cannae handle any more captain! We have to shut down for {backoff_time} seconds for repairs! (We exceeded our rate limit for the Confluent Cloud REST API)')
                time.sleep(backoff_time)
            else:
                raise HTTPError(f'HTTP Error: {response.status_code}')

            if next_url:
                response = requests.get(next_url, auth=auth)
                api_request_counter += 1

        except KeyboardInterrupt:
            break
        except HTTPError as e:
            print(f'HTTP error: {e}')
            break
        except ValueError as e:
            print(f"Value Error: {e}")
            break
    producer.flush()

    end_time = time.time()
    duration = end_time - start_time
    print("Start time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)))
    print("End time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)))
    print("Duration:", duration, "seconds")
    print('Read', data_row_counter, 'data items from the REST API in ', api_request_counter, 'requests')


main()
