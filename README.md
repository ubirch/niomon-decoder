# Niomon Decoding System
This microservice verifies and decodes ubirch protocol packets (UPPs) contained within kafka records.
The decoded messages are MessageEnvelopes that contain Protocol Messages.

## Development
Practically all the code is contained in [MessageDecodingMicroservice](src/main/scala/com/ubirch/decoding/MessageDecodingMicroservice.scala).
