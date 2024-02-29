# GMQ
The Gopher Message Queue

![](GMQGopher.png)

# What is this
A straightforward, dead simple Pub / Sub message broker.

The only configuration you need to worry about is specifying a port when running it (for now, you have to do that in the code).

# Usage
GMQ runs on TCP, it expects inputs to be plain text JSON of this structure:
```
{
"method" : "pub",
"chan" : 1,
"msg" : {"id" : "UUID", "routingkey" : "goQueue", "body": "someString"}
}
```


 ## Structure

#### `method` : possible values : pub / sub / unsub / new / del

`pub` : publishes the message in the payload into the routingKey queue, if the queue does not exist, it will create it

`sub` : subs to the routingKey queue, if the queue does not exist, will return an err

`unsub` : unsubs from the routingKey queue. no op if queue does not exist or client is not subbed

`new` : creates a new queue named as routingKey, no op if queue already exists

`del` : deletes a queue, no op if queue does not exist. Will notifiy all subs of the queue of deletion event

`queues` : returns a list of all available queues

#### `chan` : non zero, non negative int, signifies the channel id for multiplexing in TCP

#### `msg` : 

```
{"id" : "UUID", "routingkey" : "goQueue", "body": "someString"}
```

`id` : UUID - 128 bits long, has to be generated at client level

`routingkey` : name of the queue

`body` : (optional), plain text field containing the actual message, only required on pub operations, otherwise send empty str 


# Idea behind GMQ
This is a project I have developed to challenge my recentlyy acquired knowledge of distributed systems, It is not designed to be a full fledged message broker (at least not for now), a lot of features are missing, like permissions (anyone can publish / subscribe to any queue), no message persistence (everything is stored in memory), no retries on failure / dead queue managment (I intend to implement this), absolutely 0 logging except for STDOUT to the console.
It is a tool that may help people be introduced to message queue services on a surface level, or for testing in development environments.

# Contributing
Open an issue, or directly 'push' (ha!) and pull.
