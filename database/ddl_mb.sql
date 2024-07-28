-- Create table for Messages - Socket Queue
CREATE TABLE IF NOT EXISTS "Messages_Socket_Queue" (
    "Message_ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "Data" TEXT
);

-- Create table for Topics
CREATE TABLE IF NOT EXISTS "Topics" (
    "Topic_Name" TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS MessagesToSend (
    SubscriberID INTEGER NOT NULL,
    Data TEXT NOT NULL,
    FOREIGN KEY (SubscriberID) REFERENCES Subscriber(SubscriberID)
);