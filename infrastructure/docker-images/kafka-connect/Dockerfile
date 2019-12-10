FROM confluentinc/cp-kafka-connect

COPY ./connectors/jcustenborder-kafka-connect-spooldir-1.0.41 /usr/share/confluent-hub-components
COPY ./connectors/confluentinc-kafka-connect-jdbc-5.4.0-beta1 /usr/share/confluent-hub-components

COPY ./deploy-connectors.sh ./
