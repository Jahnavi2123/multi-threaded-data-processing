
# Multi-threaded Data Processing System

Assignment 6 – Java and Go Concurrency

## Overview

This project implements a multi-threaded data processing system
in Java and Go.

Both implementations process 20 tasks using three concurrent
workers. Each task calculates the square of an input value,
simulates processing delay, and saves the result to a CSV file.

## Features

- Shared task queue
- Three concurrent workers
- Thread-safe result collection
- Simulated processing delay
- Worker lifecycle logging
- Error handling and resource cleanup
- Graceful worker termination
- Duplicate and missing task detection
- CSV output

## Java Implementation

Java uses:

- BlockingQueue
- ExecutorService
- Synchronized result collection
- Poison-pill termination
- try-catch exception handling
- java.util.logging

### Run Java

cd java
javac DataProcessingSystem.java
java DataProcessingSystem

Requires Java 17 or later.

## Go Implementation

Go uses:

- Goroutines
- Channels
- sync.WaitGroup
- Explicit error handling
- defer statements
- log package

### Run Go

cd go
go run main.go

### Race Detection

go run -race main.go

## Expected Results

Each implementation produces:

- 20 submitted tasks
- 20 saved results
- 20 unique task IDs
- Correct squared values
- results.csv

Worker execution order may vary because processing is concurrent.

## Project Structure

java/         Java implementation
go/           Go implementation
screenshots/  Code and output screenshots

## Concurrency Comparison

Java uses a fixed thread pool with a synchronized blocking
queue and result collection.

Go uses lightweight goroutines and channels to communicate
tasks and results between workers.

Both implementations wait for all workers to finish before
validating and saving results.
