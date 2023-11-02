#!/usr/bin/env python
import json
import time
import configargparse

import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from requests.exceptions import HTTPError


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


# def build_producer_properties():
#
# def build_schema_registry_properties():
#
# def build_request_properties():


def producer_config(args):
    return {
        'bootstrap.servers': args.bootstrap_servers,
        'security.protocol': args.security_protocol,
        'sasl.mechanisms': args.sasl_mechanisms,
        'sasl.username': args.sasl_username,
        'sasl.password': args.sasl_password
    }


def schema_config(args):
    return {
        'url': args.schema_registry_url,
        # 'basic.auth.credentials.source': args.basic_auth_credentials_source,
        'basic.auth.user.info': args.basic_auth_user_info
    }


def request_params(args):
    params = {
        'start_date': args.start_date,
        'end_date': args.end_date
    }
    try:
        params['page_size'] = args.page_size
    except AttributeError:
        pass
    return params


def main(args):
    # Metrics
    start_time = time.time()
    print('Confluent Billing Exporter')
    print("Start time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)))
    api_request_counter = 0
    data_row_counter = 0
    produce_to_kafka_counter = 0

    # Get the schemas we'll use for the billing line items
    with open("billing_data_schema.json") as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient(schema_config(args))

    # Set the key and value serialisers
    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client)

    # Create Producer instance
    producer = Producer(producer_config(args))

    # Set the outbound topic from the value in the config file
    topic = args.topic

    # Create the initial request to the REST API. Subsequent requests will use the pagination feature
    response = requests.get(args.rest_url,
                            params=request_params(args),
                            auth=(args.rest_api_key, args.rest_api_secret))
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
                    produce_to_kafka_counter += 1
            elif response.status_code == 429:
                backoff_time += 1
                print(
                    f'Confluent Cloud Rate Limit exceeded, backing off for {backoff_time} seconds')
                time.sleep(backoff_time)
            else:
                raise HTTPError(f'HTTP Error: {response.status_code}')

            if next_url:
                response = requests.get(next_url, auth=(args.rest_api_key, args.rest_api_secret))
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
    print("End time:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time)))
    print("Duration:", duration, "seconds")
    print('Read', data_row_counter, 'data items from the REST API in ', api_request_counter, 'requests')
    print('Produced', produce_to_kafka_counter, 'messages to Kafka topic', topic)


# main()
if __name__ == '__main__':
    # Set up the configuration argument parser
    # client.properties includes everything needed to connect to Kafka
    # exporter.properties contains everything else, including REST API Key, topic and request parameters
    p = configargparse.ArgParser(default_config_files=['client.properties', 'exporter.properties'])
    # Arguments for config files
    p.add_argument('--file',
                   is_config_file=True,
                   help='Path to a file containing properties',
                   env_var='CFLTBE_CONFIG_FILE')

    # Arguments from client.properties
    p.add_argument('--bootstrap.servers',
                   # required=True,
                   dest='bootstrap_servers',
                   help='The Bootstrap Servers values for the Kafka cluster being produced to',
                   env_var='CFLTBE_BOOTSTRAP_SERVERS')
    p.add_argument('--security.protocol',
                   default='SASL_SSL',
                   dest='security_protocol',
                   help='The security.protocol setting',
                   env_var='CFLTBE_SECURITY_PROTOCOL')
    p.add_argument('--sasl.mechanisms',
                   default='PLAIN',
                   dest='sasl_mechanisms',
                   help='The sasl.mechanisms setting',
                   env_var='CFLTBE_SASL_MECHANISMS')
    p.add_argument('--sasl.username',
                   # required=True,
                   dest='sasl_username',
                   help='An API Key for a service account with write access to the targer cluster and topic',
                   env_var='CFLTBE_CLUSTER_API_KEY')
    p.add_argument('--sasl.password',
                   # required=True,
                   dest='sasl_password',
                   help='The secret for the API Key',
                   env_var='CFLTBE_CLUSTER_API_SECRET')
    p.add_argument('--session.timeout.ms',
                   default='45000',
                   dest='session_timeout_ms',
                   help='The session timeout for consumer sessions',
                   env_var='CFLTBE_SESSION_TIMEOUT_MS')
    p.add_argument('--schema.registry.url',
                   # required=True,
                   dest='schema_registry_url',
                   help='The url for the Confluent Schema Registry',
                   env_var='CFLTBE_SCHEMA_REGISTRY_URL')
    p.add_argument('--basic.auth.credentials.source',
                   default='USER_INFO',
                   dest='basic_auth_credentials_source',
                   help='The basic auth credentials source',
                   env_var='CFLTBE_SCHEMA_REGISTRY_CREDENTIALS_SOURCE')
    p.add_argument('--basic.auth.user.info',
                   # required=True,
                   dest='basic_auth_user_info',
                   help='The Basic Auth credentials for the Confluent Schema Registry, in the form of '
                        '"API_KEY:API_SECRET"',
                   env_var='CFLTBE_SCHEMA_REGISTRY_BASIC_AUTH')

    # Arguments from exporter.properties
    p.add_argument('--topic',
                   # required=True,
                   help='The topic to produced to',
                   env_var='CFLTBE_TOPIC')
    p.add_argument('--rest-url',
                   default='https://api.confluent.cloud/billing/v1/costs',
                   help='The url of the Confluent Billing REST API',
                   env_var='CFLTBE_REST_URL')
    p.add_argument('--rest-api-key',
                   # required=True,
                   help='A Confluent Cloud API Key for a user with the Organization Admin rolebinding',
                   env_var='CFLTBE_REST_API_KEY')
    p.add_argument('--rest-api-secret',
                   # required=True,
                   help='The API Secret corresponding to the API Key',
                   env_var='CFLTBE_REST_API_SECRET')
    p.add_argument('--start-date',
                   # required=True,
                   help='The topic to produced to',
                   env_var='CFLTBE_START_DATE')
    p.add_argument('--end-date',
                   # required=True,
                   help='The topic to produced to',
                   env_var='CFLTBE_END_DATE')
    p.add_argument('--page-size',
                   help='The topic to produced to',
                   env_var='CFLTBE_PAGE_SIZE')
    main(p.parse_args())
