# General description

The application consists of three components: sensors, message broker and subscriber.
The sensors generate randomized data for temperature and UV radiation. This data is then sent to the message broker.

The message broker receives the data from the sensors and is responsible for distributing the data to the relevant subscribers.

At the start of the application, the subscribers specify which sensors and data they wish to receive and define this relationship with the message broker. They then receive the requested content.
 
## Communication

The communication is excecuted using sockets. For this we implemented a parent socket class which defines the basic functions both child classes need to build up and sustain a connection. These two child classes are responsible for sending and recieving data and therefore have different functions defined tailored to the specific needs.

### Sending socket

The sending socket is used by the sensors to send their data to the messagebroker which then also uses the sending socket to transmit the data to the subscribers.
This socket uses the UID of the respective sensor, the sequence number labeling the order of the message as well as the transmittable data.

### Recieving socket

The recieving socket is used by the messagebroker to recieve the sensor's data as well as by the subscribers to recieve the messagebroker's data. IT uses the same parameters for these processes as the sending socket. 

## Transmition Process

The current data sitting at a sensor waiting for transmition will generally be placed into a queue. When transmitting the data it will be removed from the queue. If the transmition fails the data will be retransmitted using the persisted data explained later on.

After transmition, that data will then be persisted in the messagebroker's database. The messagebroker now sorts the recieved data into the seperate queues for each subscriber depending on the subscriptions they made. The data of that queue will than transmitted using the same logic.

### Protocol

In order to ensure a reliable communication we introduced acknowledgements. Whenever data is transmitted, a acknowledgement from the recieving party will be sent to the sender stating the successfull and correct transmittion of the data following a second acknowledgement confirming the reception of the first acknowledgement.
An incorrect transmition of the data or missing reception of the first acknowledgement will leed to a retransmission.

Ding mit 2ACK

A hashed checksum of the transmitted data and timestamp is used to uniquely identify the package during the processes.

### Persistence

In order to ensure persistence we used databases storing the to be send data so that it in case of a technical error doesn't get lost. 

The sensor will store it's acquried data in a database before transmition. Once the sensor recieved the acknowledgement of the messagebroker the piece of data, identified by the checksum, will be deleted from the database table.

The same logic applies to the data transmition process between the messagebroker and the subscribers.
  
## Execution

Before execting the application the number of build sensors and subscribers can be defined in the main file. The confoguration file additionally offers the posibility of defining the maximum time for resending a failed message. The displayed logging information can also be adjusted.

The application can then be executed starting the main-funtion.

