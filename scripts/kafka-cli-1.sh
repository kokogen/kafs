#!/bin/bash
/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic troll --partitions 2
/bin/kafka-topics --bootstrap-server localhost:9092 --describe --topic troll