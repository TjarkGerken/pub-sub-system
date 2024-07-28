-- Create table for Messages - Receiving in the Subscriber database
CREATE TABLE IF NOT EXISTS MessagesReceiving (
    MessageID INTEGER PRIMARY KEY AUTOINCREMENT,
    Data TEXT
);

-- Create table for Subscribed To in the Subscriber database
CREATE TABLE IF NOT EXISTS SubscribedTo (
    TopicName TEXT PRIMARY KEY
);
