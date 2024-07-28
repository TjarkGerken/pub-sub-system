-- Create table for Messages - Receiving in the Subscriber database
CREATE TABLE "Messages_Receiving" (
    "Message_ID" INTEGER PRIMARY KEY AUTOINCREMENT,
    "Data" TEXT
);

-- Create table for Subscribed To in the Subscriber database
CREATE TABLE "Subscribed_To" (
    "Topic_Name" TEXT PRIMARY KEY
);
