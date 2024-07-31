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

## Data Flow Diagram and Process Description

![Data Flow Diagram](pictures/Application_schema.svg)

### Data Transmission from Sensor to Message Broker
### 1. Sending Data: The sensor sends data to the message queue (MQ) of the message broker.
-	*Error Handling:* If the data cannot be sent, the operation is retried.
### 2.Storing Data in the Message Broker: The data is stored in the MQ of the MB.
-	*Error Handling:* If the header checksum is incorrect, no acknowledgment (ACK) is sent, and the sensor waits for the entire packet to be resent.
-	*Solution:* The sensor waits X seconds for the first ACK. If it does not arrive, the sensor resends the data Y times. After Y unsuccessful attempts, the message is discarded.
### 3. Receiving the First ACK: When the data has been correctly received by the MB, it sends an ACK to confirm to the sensor.
-	*Error Handling:* If the ACK does not arrive, the sensor resends the data, assuming the transmission to the MB was unsuccessful.
-	*Solution:* Upon receiving the ACK, the data is deleted from the MQ of the sensor, and a second ACK is sent to the MB.
### 4. Receiving the Second ACK: The MB validates the successful receipt and sends a second ACK back to the sensor.
-	*Error Handling:* If the second ACK does not arrive, the checksum is not removed from the temporary storage.
###Data Transmission from Message Broker to Subscriber
### 1. Sending Data: The MB sends the data to the MQ of the subscriber.
-	*Error Handling:* If the data does not arrive completely, no ACK is sent to the MB.
### 2. Storing Data in the Subscriber: The data is stored in the MQ of the subscriber.
-	*Error Handling:* If the MB does not receive an ACK, it resends the data after X seconds Y times. After Y unsuccessful attempts, the message is discarded.
### 3. Receiving the First ACK: When the data has been correctly received by the subscriber, it sends an ACK to the MB.
-	*Error Handling:* If the ACK does not arrive, the MB resends the data.
-	*Solution:* Upon receiving the ACK, the data is deleted from the MQ of the MB, and a second ACK is sent to the subscriber.
### 4. Receiving the Second ACK: The subscriber validates the successful receipt and stores the data in a file.
-	*Solution:* The data is sequentially stored and processed in the correct order.
## Important Notes
-	**Order of Data Processing:** The order of data transmission and processing is strictly maintained to ensure that the data is stored correctly and completely.
-	**Checksums:** Checksums are used to verify the integrity of the received data and to ensure no duplicate data is stored.
-	**Data Persistence:** The use of SQLite3 ensures data persistence even if a component fails and is restarted.
## Summary 
This project ensures that data is reliably and accurately transmitted between a sensor, a message broker, and a subscriber. By implementing comprehensive error handling mechanisms and using SQLite3 for data storage, the integrity and order of the data are maintained. The choice of SQLite3 as the database solution was made because it is a built-in solution in Python and does not require additional installation, simplifying implementation.

