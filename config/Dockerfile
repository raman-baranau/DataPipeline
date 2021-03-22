# Add BQ and JDBC connectors
FROM confluentinc/cp-kafka-connect:6.0.2
COPY ./wepay-kafka-connect-bigquery-2.1.0 /usr/share/java/wepay-kafka-connect-bigquery-2.1.0
COPY ./confluentinc-kafka-connect-jdbc-10.0.2 /usr/share/java/confluentinc-kafka-connect-jdbc-10.0.2
COPY gcp_cred.json /usr/share/java/