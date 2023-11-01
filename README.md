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

## To make it work
* Update the `example.properties` file with the settings for your Confluent Cloud account and the name of the target topic
* Update the `example.properties` file with the dates to be queried in the [request] section. Note that the API will not serve more than a month at a time and that the end date is exclusive. That is, to download all of the data for the month of October, you would specify `start_date=2023-10-01` and `end_date=2023-11-01`.
* Execute the script
```
python3 ./confluent-billing-exporter.py ./example.properties 
```

## References

The Confluent Cloud Billing API: https://docs.confluent.io/cloud/current/api.html#tag/Costs-(billingv1)
Documentation on how to retrieve costs for a range of dates: https://docs.confluent.io/cloud/current/billing/overview.html#retrieve-costs-for-a-range-of-dates

## To Do
* Enable startup parameters to be supplied by environment, command line or config file, to make it more flexible
* Make the schemas more conformant with the documented API
* More/better error handling