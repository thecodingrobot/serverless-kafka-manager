Severless Kafka manager
---
Interact with your Kafka cluster.


## API
### Retrieving messages
**`GET /consume/{topic}`**

**`GET /consume/{topic}/partition/{partition}`**

**`GET /consume/{topic}/partition/{partition}/offset/{offset}`**

*Optional query parameters:*
* `groupId` - set the consumer group ID. The default fallback is random UUID per request.  
*Example:*  
`GET /consume/my_topic?groupId=consumer_1`
---
* `hex` - Display hexdump of the payload.  
*Example:*
    ```
    GET /consume/my_topic?hex

    Metadata:
    Topic:     topic_1
    Group ID:  1
    Offset:    0
    Partition: 1
    Key:       b'my_key'
    
    
    Message:
    00000000: 48 65 6C 6C 6F 57 6F 72  6C 64                    HelloWorld
    ````
---
 * `json` - Use Avro to deserialize the payload.  
 *Example:*  
  ```GET /consume/my_topic?json```

### Copy messages between topics
**``POST /copy/{from_topic}/{to_topic}``**


## Deploy
*TODO*  
Included native libraries required for connecting to Kafka.
Compiled on AWS Linux to assure compatibility with Lambda. 
 - librdkafka 0.9.5
 - liblz4
 - libsasl2

`$ serverless deploy`