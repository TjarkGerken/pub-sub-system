-- Create table for Messages - Receiving in the Subscriber database
CREATE TABLE IF NOT EXISTS "Messages_Receiving" (
    "Message_ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "Data" TEXT
);

-- Create table for Subscribed To in the Subscriber database
CREATE TABLE IF NOT EXISTS "Subscribed_To" (
    "Topic_Name" TEXT PRIMARY KEY
);
