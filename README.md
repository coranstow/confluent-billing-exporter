# Confluent Cloud Billing Exporter

## Overview
This project demonstrates a method of importing billing information from the Confluent Cloud REST API into a Kafka topic. The script will retrieve all billing line items from the defined period, unwrapping them from the Confluent Cloud REST responses and submitting just the data lines to kafka as individual Kafka messages.

Each line item is produced to Kafka as JSON and the schema is registered with the Schema Registry. Each message has a key that is a string-serialised JSON object containing the ID, Start Date and Granularity of the billing line item. This should allow us to store this data in compacted topics over the long term.

The items in the [request] section of the properties file are passed in directly as parameters of the REST API request. If the response exceeds the page size, the script will page through all of the response pages. If the script exceeds its rate limit, it will back off and retry. Other errors are not handled.

## Requirements

* Python 3
* A Confluent Cloud API Key for a service user with the Organization Admin role (You must be Organization Admin to read billing information)
* A Confluent Cloud Environment with a schema registry
* An API Key for your environment's schema registry API
* A Confluent Cloud cluster
* A Topic to write to
* An API Key for the cluster, that has at least the DeveloperWrite role binding to the target topic
* Python Libraries
    * ConfigArgParse >= 1.7
    * jsonschema >= 4.19.2
    * python-dateutil >= 2.8.2
    * confluent-kafka >= 2.3.0
    * requests >= 2.31.0

## Required arguments

The program requires a number of variables to make it work, but I've used [ConfigArgParse] (https://pypi.org/project/ConfigArgParse/) to try and make it very flexible. For example, you should be able to supply a file containing all of the parameters that rarely change, such as the connection settings, and then individual parameters for that apply to a single run, such as the start and end dates.
The program will default to looking for files called `client.properties` and `exporter.properties` and a config file can be passed using the `--file` flag. Every parameter can also be passed on the command line or as an environment variable. Run `python3 ./confluent-billing-exporter.py --help` to see the flags and environment variables. As long as all required variables are discovered in the environment, command line or a file the program will run. The order of precedence is that command line values override environment variables, which override config file values, which override defaults.


## To make it work
* Update the `example.properties` file with the settings for your Confluent Cloud account and the name of the target topic
* Update the `example.properties` file with the dates to be queried in the [request] section. Note that the API will not serve more than a month at a time and that the end date is exclusive. That is, to download all of the data for the month of October, you would specify `start-date=2023-10-01` and `end-date=2023-11-01`.
* Execute the script
```
python3 ./confluent-billing-exporter.py --file ./example.properties 
```

## References

The Confluent Cloud Billing API: https://docs.confluent.io/cloud/current/api.html#tag/Costs-(billingv1)
Documentation on how to retrieve costs for a range of dates: https://docs.confluent.io/cloud/current/billing/overview.html#retrieve-costs-for-a-range-of-dates

## To Do
* Make the schemas more conformant with the documented API
* More/better error handling
* Dockerise it