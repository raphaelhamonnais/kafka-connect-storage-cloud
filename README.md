# Kafka Connect Suite of Cloud Storage Connectors
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud.svg?type=shield)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud?ref=badge_shield)


*kafka-connect-storage-cloud* is a suite of [Kafka Connectors](http://kafka.apache.org/documentation.html#connect)
designed to be used to copy data between Kafka and public cloud stores, such as Amazon S3. The currently available connectors are listed below:

## Kafka Connect Sink Connector for Amazon Simple Storage Service (S3)

Documentation for this connector can be found [here](http://docs.confluent.io/current/connect/connect-storage-cloud/kafka-connect-s3/docs/index.html).

## Kafka Connect Sink Connector for Google Cloud Storage (GCS)

### Features present in the S3 connector and not in GCS one

- Multipart upload
    - Doesn't work as of 2019-01-29
        - The connector logic loses bytes and the `compose` method used to combine small parts leaves those parts present in the buckets. 
    - Temporarily disabled for an _infinite_ buffer that will throw OOMs if to many bytes are written.
    - This feature must be implemented at some point.
- Specify custom credentials
    - Using a specific key or project ID is not implemented: the GCS client is connecting using the default credentials present on the node it's running on.
    - To implement only if needed.
- ACL (Access Control List)
    - Read the [ACL GCP documentation](https://cloud.google.com/storage/docs/access-control/lists) for more information
    - To implement only if needed.
- S3 Region / GCS Location
    - According to [the documentation](https://cloud.google.com/storage/docs/locations), the location can only be specified when creating a bucket and not at the GCS client level.
    - Kafka Connect doesn't handle the bucket creation, so this feature shouldn't be needed by the GCS connector.
    - To implement only if needed.
- Server Side Encryption (SSE)
    - S3 connectors related configurations
        - SSEA or Server Side Encryption Algorithm
        - SSE customer key
        - SSE AWS Key Management Service (AWS-KMS) key to be used for server side encryption of the S3 objects
    - [GCS encrypts data on the server side by default](https://cloud.google.com/storage/docs/encryption/)
    - To implement only if needed.
- S3 accelerated endpoint
    - S3 specific feature.

Only the multipart upload feature must be implemented at some point in the GCS Connector.

# Development

To build a development version you'll need a recent version of Kafka 
as well as a set of upstream Confluent projects, which you'll have to build from their appropriate snapshot branch.
See [the kafka-connect-storage-common FAQ](https://github.com/confluentinc/kafka-connect-storage-common/wiki/FAQ)
for guidance on this process.

You can build *kafka-connect-storage-cloud* with Maven using the standard lifecycle phases.


# Contribute

- Source Code: https://github.com/confluentinc/kafka-connect-storage-cloud
- Issue Tracker: https://github.com/confluentinc/kafka-connect-storage-cloud/issues


# License

This project is licensed under the [Confluent Community License](LICENSE).


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud.svg?type=large)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud?ref=badge_large)
