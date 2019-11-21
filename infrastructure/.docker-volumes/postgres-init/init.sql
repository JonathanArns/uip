CREATE USER kafka_connect WITH PASSWORD 'kafka_connect';
CREATE DATABASE kafka_connect;
GRANT ALL PRIVILEGES ON DATABASE kafka_connect TO kafka_connect;
