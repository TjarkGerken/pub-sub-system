DROP TABLE IF EXISTS Subscriber;
CREATE TABLE Subscriber (
    SubscriberID INTEGER PRIMARY KEY AUTOINCREMENT,
    Address TEXT NOT NULL,
    Port INT NOT NULL,
    Topic TEXT NOT NULL,
    UNIQUE (Address, Port, Topic)
);

DROP TABLE IF EXISTS MessagesToSend;
CREATE TABLE IF NOT EXISTS MessagesToSend (
    MessageID INTEGER PRIMARY KEY AUTOINCREMENT,
    SubscriberID INTEGER NOT NULL,
    Data TEXT NOT NULL,
    FOREIGN KEY (SubscriberID) REFERENCES Subscriber(SubscriberID),
    UNIQUE(SubscriberID, Data)
);