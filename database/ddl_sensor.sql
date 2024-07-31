-- Creates a table for the queue of the messages the sensor has to send
CREATE TABLE IF NOT EXISTS MessagesToSend (
    MessageID INTEGER PRIMARY KEY AUTOINCREMENT,
    Data TEXT
);
