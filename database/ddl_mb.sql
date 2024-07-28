CREATE TABLE IF NOT EXISTS Subscriber (
    SubscriberID INTEGER PRIMARY KEY AUTOINCREMENT,
    Address TEXT NOT NULL,
    Port INT NOT NULL,
    Topic TEXT NOT NULL,
    UNIQUE (Address, Port, Topic)
);

CREATE TABLE IF NOT EXISTS MessagesToSend (
    SubscriberID INTEGER NOT NULL,
    Data TEXT NOT NULL,
    FOREIGN KEY (SubscriberID) REFERENCES Subscriber(SubscriberID)
);