-- Create table for Messages - Socket Queue
CREATE TABLE IF NOT EXISTS "Messages_Socket_Queue" (
    "Message_ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "Data" TEXT
);

-- Create table for Topics
CREATE TABLE IF NOT EXISTS "Topics" (
    "Topic_Name" TEXT PRIMARY KEY
);

-- Create table for Messages - MB Queue
CREATE TABLE IF NOT EXISTS "Messages_MB_Queue" (
    "Message_ID" INTEGER,
    "Topic_Name" TEXT,
    FOREIGN KEY ("Message_ID") REFERENCES "Messages_Socket_Queue" ("Message_ID"),
    FOREIGN KEY ("Topic_Name") REFERENCES "Topics" ("Topic_Name")
);

-- Create table for Subscriber
CREATE TABLE IF NOT EXISTS "Subscriber" (
    "Address" TEXT,
    "Port" INTEGER,
    "Type" TEXT,
    PRIMARY KEY ("Address", "Port")
);

-- Create table for Delivered
CREATE TABLE IF NOT EXISTS "Delivered" (
    "Subscriber_ID" TEXT,
    "Message_ID" INTEGER,
    "Delivered" BOOLEAN,
    FOREIGN KEY ("Subscriber_ID") REFERENCES "Subscriber" ("Address"),
    FOREIGN KEY ("Message_ID") REFERENCES "Messages_Socket_Queue" ("Message_ID")
);

-- Create table for Subscribed To
CREATE TABLE IF NOT EXISTS "Subscribed_To" (
    "Subscriber_ID" TEXT,
    "Topic_ID" TEXT,
    FOREIGN KEY ("Subscriber_ID") REFERENCES "Subscriber" ("Address"),
    FOREIGN KEY ("Topic_ID") REFERENCES "Topics" ("Topic_Name")
);
