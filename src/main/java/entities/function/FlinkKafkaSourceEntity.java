package entities.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import com.cisco.webex.datahub.client.lineage.request.LineageRequest.LineageRequestBuilder;

import entities.NodeEntity;
import utils.ClassUtils;
import utils.DataHubUtils;

public class FlinkKafkaSourceEntity extends NodeEntity<KafkaSource> {

    public FlinkKafkaSourceEntity(Object node) {
        super(node);
    }

    @Override
    public void add2Lineage(LineageRequestBuilder builder) {
        KafkaSource kafkaSource = this.node;
        try {
            Properties props = ClassUtils.getFiledValue(kafkaSource, "props", Properties.class);
            String bootStrapServer = null;
            if (Objects.nonNull(props)) {
                bootStrapServer = props.getProperty("bootstrap.servers");
            }
            if (StringUtils.isBlank(bootStrapServer)) {
                return;
            }
            KafkaSubscriber topicsDescriptor = ClassUtils.getFiledValue(kafkaSource,
                "subscriber", KafkaSubscriber.class);
            if (ClassUtils.isClassInstance(topicsDescriptor, "org.apache.flink.connector.kafka.source.enumerator.subscriber.TopicListSubscriber")) {
                List<String> topics = ClassUtils.getFiledValue(topicsDescriptor, "topics", List.class);
                if (Objects.nonNull(topics) && topics.size() > 0) {
                    for (String topic : topics) {
                        builder.sourceKafka(topic, new ArrayList<>(), DataHubUtils.getCluster(bootStrapServer));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
