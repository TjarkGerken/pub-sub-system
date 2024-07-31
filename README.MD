## General description

The application consists of three components: sensors, message broker and subscribers.  
The sensors generate randomized data for temperature and UV radiation. This data is then sent to the message broker.  
The message broker receives the data from the sensors and is responsible for distributing the data to the relevant subscribers.

At the start of the application, the subscribers specify which sensors and data they wish to receive and define this 
relationship with the message broker. They then receive the requested content.
 
## Communication

The communication is executed using sockets. For this we implemented a parent socket class which defines the basic functions  
both child classes need to build up and sustain a connection.  
These two child classes are responsible for sending and receiving data and therefore have different functions defined tailored to the specific needs.

### Data structure
  
![Data structure](pictures/Data_structure.svg)

### Sending socket

The sending socket is used by the sensors to send their data to the message broker  
which then also uses the sending socket to transmit the data to the subscribers.

This socket uses the UID of the respective sensor, the sequence number labeling the order of the message as well as 
the transmittable data. 

It follows the sequence of sending the data, waiting for the acknowledgement discussed further below  
and then also sending an acknowledgement with respective error handling processes in between.

### Receiving socket

The receiving socket is used by the message broker to receive the sensor's data as well as by the subscribers to receive
the message broker's data. 

It uses the same parameters for these processes as the sending socket.

In contrast to the sending socket it follows the sequence of listening for data,  
sending an acknowledgement after reception and then waiting for the second acknowledgement again.

## transmission Process

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

### Application schema

Extended information of the execution order can be found in the figure below and the code documentation.

![Application schema](pictures/Application_schema.svg)

## Execution

Before executing the application, the number of build sensors and subscribers can be defined in the main file.  
The configuration file additionally offers the possibility of defining the timespans for resending a failed message.  
The displayed logging information can also be adjusted.

The application can then be executed starting the main-function.
