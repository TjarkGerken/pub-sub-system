-- Create table for Messages - Receiving in the Subscriber database
CREATE TABLE MessagesReceiving (
    MessageID INTEGER PRIMARY KEY AUTOINCREMENT,
    Data TEXT
);

-- Create table for Subscribed To in the Subscriber database
CREATE TABLE SubscribedTo (
    TopicName TEXT PRIMARY KEY
);
