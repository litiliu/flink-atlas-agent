package entities.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;

import com.cisco.webex.datahub.client.lineage.request.LineageRequest.LineageRequestBuilder;

import entities.NodeEntity;
import utils.ClassUtils;
import utils.DataHubUtils;

public class FlinkKafkaSinkEntity extends NodeEntity<KafkaSink> {

    public FlinkKafkaSinkEntity(Object node) {
        super(node);
    }

    @Override
    public void add2Lineage(LineageRequestBuilder builder) {
        KafkaSink kafkaSink = this.node;
        try {
            Object recordSerializer = ClassUtils.getFiledValue(kafkaSink, "recordSerializer", Object.class);
            String topic = null;
            if (ClassUtils.isClassInstance(recordSerializer, "org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaRecordSerializationSchema")) {
                topic = ClassUtils.getFiledValue(recordSerializer, "topic", String.class);
            }
            Properties kafkaProducerConfig = ClassUtils.getFiledValue(kafkaSink,
                "kafkaProducerConfig", Properties.class);
            String bootStrapServer = null;
            if (Objects.nonNull(kafkaProducerConfig)) {
                bootStrapServer = kafkaProducerConfig.getProperty("bootstrap.servers");

            }
            if (StringUtils.isNotEmpty(bootStrapServer) && StringUtils.isNotEmpty(topic)) {
                builder.successorKafka(topic,new ArrayList<>(), DataHubUtils.getCluster(bootStrapServer));
                //topic flink-test-1
                // bootStrapServer bt1
            }

        } catch (Throwable t) {
            // TODO
        }
    }


}
