# HTML Table Ingestion Platform

A scalable ingestion pipeline that extracts structured data from web tables
and streams records through Kafka into relational storage.

## Features

- Automatic HTML table extraction
- Schema inference
- Kafka streaming pipeline
- Dynamic MySQL storage
- Dashboard based ingestion console
- Database viewer
- Docker based deployment

## Architecture

Frontend Dashboard → Go Backend → Kafka Producer → Kafka Stream → Consumer → MySQL Storage

## Tech Stack

- GoLang
- Apache Kafka
- MySQL
- Docker
- REST APIs
- HTML CSS frontend

## Setup Instructions

### Requirements

- Docker
- Docker Compose
