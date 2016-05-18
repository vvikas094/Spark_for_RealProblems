#!/bin/bash
spark-submit --num-executors 20 --master yarn-client sparkProgram.py hdfs://hadoop2-0-0/data/twitter/*
