# DHBW Mannheim - Distributed Systems
This repository contains the code for the project of the Distributed Systems course at DHBW Mannheim. The aim was to 
develop a system for data transmission between sensors, a message broker and subscribers ensuring data integrity.

## Contributors
- Yanick Bedel (8424886)
- Tjark Gerken (8692717)
- Carlo Rinderer (1902925)
- Niklas Seither (4253802)
- David Simon (1893552)

## Tech-Stack
- Python 3.12

## Execution

To run the project locally, you can either run the `main.py` file or start each component in a separate terminal window.

```bash
# To run the main.py file, move to the project directory and execute the following command
python main.py

# To run the components separately, execute the following commands in separate terminal windows
python -m classes.sensor
python -m classes.message_broker
python -m classes.subscriber
```

To configure the application you can adjust the `configuration.py` file.

## General description

The application consists of three components: sensors, message broker and subscribers. The sensors generate randomized 
data for temperature and UV radiation. This data is then sent to the message broker. The message broker receives the 
data from the sensors and is responsible for distributing the data to the relevant subscribers.

At the start of the application, the subscribers specify which sensors and data they wish to receive and define this 
relationship with the message broker. They then receive the requested content.

A screencast explaining the application can be found [here](https://youtube.com). 

## Overview
This project implements a robust system for data transmission between a sensor, a message broker (MB), and a subscriber. The system aims to ensure data integrity and prevent data loss through comprehensive error handling.
## System Components
### Sensor
-	Database:* SQLite3
-	Error:* Sensor fails before data transmission; data must be resent after a reboot.
-	Solution:* Data persistence using SQLite3.
### Message Broker (MB)
-	*Database:* SQLite3
-	*Error:* Incomplete data transmission; header checksum does not match.
-	*Solution:* Data persistence and validation of data integrity through checksums.
### Subscriber
-	*Database:* SQLite3
-	*Error:* Sensor fails before all messages are processed; data must be reloaded and processed after a reboot.
-	*Solution:* Data persistence using SQLite3.
### SQLite3
We chose SQLite3 because it is a built-in database solution in Python and does not require additional installation. This simplicity and accessibility make SQLite3 the ideal choice for our project.

 
## Communication

The communication is executed using sockets. For this we implemented a parent socket class which defines the basic 
functions both child classes need to build up and sustain a connection. These two child classes are responsible for 
sending and receiving data and therefore have different functions defined tailored to the specific needs.

### Data structure
  
![Data structure](pictures/Data_structure.svg)

### Sending socket

The sending socket is used by the sensors to send their data to the message broker which then also uses the sending 
socket to transmit the data to the subscribers.

This socket uses the UID of the respective sensor, the sequence number labeling the order of the message as well as 
the transmittable data. 

It follows the sequence of sending the data, waiting for the acknowledgement discussed further below and then also 
sending an acknowledgement with respective error handling processes in between.

### Receiving socket

The receiving socket is used by the message broker to receive the sensor's data as well as by the subscribers to receive
the message broker's data. 
It uses the same parameters for these processes as the sending socket.
In contrast to the sending socket it follows the sequence of listening for data, sending an acknowledgement after 
reception and then waiting for the second acknowledgement again.

## Transmission Process

The current data sitting at a sensor waiting for transmission will generally be placed into a queue.  
When transmitting the data it will be removed from the queue.  
If the transmission fails, the data will be retransmitted using the persisted data explained later on.

After transmission, that data will then be persisted in the message broker's database.  
The message broker now sorts the received data into the separate queues for each subscriber depending on the subscriptions they made.  
The data of that queue will then be transmitted using the same logic.

### Protocol

In order to ensure a reliable communication we introduced acknowledgements. Whenever data is transmitted, an acknowledgement  
from the receiving party will be sent to the sender stating the successful and correct transmission of the data.  
This is then followed by a second acknowledgement from the sender to the receiving party confirming the reception of the first acknowledgement.

An incorrect transmission of the data or missing reception of the first acknowledgement will lead to a retransmission.

A hashed checksum of the transmitted data and timestamp is used to uniquely identify the package during the processes.

### Persistence

In order to ensure persistence we used databases storing the to be sent data so that it doesn't get lost in case of a technical error.

The sensor will store it's acquired data in a database before transmission. Once the sensor received the acknowledgement of the message broker,  
the piece of data, identified by the checksum, will be deleted from the database table.

The same logic applies to the data transmission process between the message broker and the subscribers.

## Data Flow Diagram and Process Description

![Data Flow Diagram](pictures/Application_schema.svg)

### Data Transmission from Sensor to Message Broker
#### 1. Sending Data: The sensor sends data to the message queue (MQ) of the message broker.
-	*Error Handling:* If the data cannot be sent, the operation is retried.
#### 2.Storing Data in the Message Broker: The data is stored in the MQ of the MB.
-	*Error Handling:* If the header checksum is incorrect, no acknowledgment (ACK) is sent, and the sensor waits for the entire packet to be re-sent.
-	*Solution:* The sensor waits X seconds for the first ACK. If it does not arrive, the sensor resends the data Y times. After Y unsuccessful attempts, the message is discarded.
#### 3. Receiving the First ACK: When the data has been correctly received by the MB, it sends an ACK to confirm to the sensor.
-	*Error Handling:* If the ACK does not arrive, the sensor resends the data, assuming the transmission to the MB was unsuccessful.
-	*Solution:* Upon receiving the ACK, the data is deleted from the MQ of the sensor, and a second ACK is sent to the MB.
#### 4. Receiving the Second ACK: The MB validates the successful receipt and sends a second ACK back to the sensor.
-	*Error Handling:* If the second ACK does not arrive, the checksum is not removed from the temporary storage.
     
### Data Transmission from Message Broker to Subscriber
#### 1. Sending Data: The MB sends the data to the MQ of the subscriber.
-	*Error Handling:* If the data does not arrive completely, no ACK is sent to the MB.
#### 2. Storing Data in the Subscriber: The data is stored in the MQ of the subscriber.
-	*Error Handling:* If the MB does not receive an ACK, it resends the data after X seconds Y times. After Y unsuccessful attempts, the message is discarded.
#### 3. Receiving the First ACK: When the data has been correctly received by the subscriber, it sends an ACK to the MB.
-	*Error Handling:* If the ACK does not arrive, the MB resends the data.
-	*Solution:* Upon receiving the ACK, the data is deleted from the MQ of the MB, and a second ACK is sent to the subscriber.
#### 4. Receiving the Second ACK: The subscriber validates the successful receipt and stores the data in a file.
-	*Solution:* The data is sequentially stored and processed in the correct order.

## Key Features
-	**Order of Data Processing:** The order of data transmission and processing is strictly maintained to ensure that the data is stored correctly and completely.
-	**Checksums:** Checksums are used to verify the integrity of the received data and to ensure no duplicate data is stored.
-	**Data Persistence:** The use of SQLite3 ensures data persistence even if a component fails and is restarted.
