# spark-streaming-sample
playing around with spark 2.0 streaming

### Key concepts
- Its key abstraction is a Discretized Stream or, in short, a DStream, which represents a stream of data divided into small batches. 
- DStreams are built on RDDs, Spark’s core data abstraction.

- The code and business logic can be shared and reused between streaming, batch, and interactive processing pipelines.

Use cases:
- streaming ETL (extract transform load)
- triggers, detect anomalous behavior
- data enrichment
- complex sessions, continuous learning

### Data Firehose
- <http://www.meetup.com/meetup_api/docs/stream/2/rsvps/#websockets>

### Resources
- <https://www.datanami.com/2015/11/30/spark-streaming-what-is-it-and-whos-using-it/>
