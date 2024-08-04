-- Creates a table for the queue of the received messages from the socket
CREATE TABLE IF NOT EXISTS MessageSocketQueue (
    MessageID INTEGER PRIMARY KEY AUTOINCREMENT,
    Data TEXT
);

-- Creates a table for the stored checksums of received messages
CREATE TABLE IF NOT EXISTS Checksums (
     DicKey TEXT PRIMARY KEY,
     Checksum TEXT NOT NULL
);