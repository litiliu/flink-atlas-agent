package entities;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleInputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;

import com.cisco.webex.datahub.client.lineage.request.LineageRequest.LineageRequestBuilder;

import entities.function.FlinkKafkaSinkEntity;
import entities.function.FlinkKafkaSourceEntity;
import entities.function.GenericJdbcSinkFunctionEntity;
import entities.function.GenericJdbcSourceFunctionEntity;
import utils.ClassUtils;

import java.util.Objects;

public class StreamNodeEntity extends NodeEntity<StreamNode> {

    public StreamNodeEntity(StreamNode streamNode) {
        super(streamNode);
    }

    @Override
    public void add2Lineage(LineageRequestBuilder lineageRequestBuilder) {
        try {
            StreamOperatorFactory operatorFactory = node.getOperatorFactory();
            if (operatorFactory instanceof SourceOperatorFactory) {
                Source source = ClassUtils.getFiledValue((operatorFactory), "source", Source.class);
                if(Objects.nonNull(source)){
                    switch (source.getClass().getName()) {
                        case FLINK_CONNECTOR_KAFKA_SOURCE:
                            new FlinkKafkaSourceEntity(source).add2Lineage(lineageRequestBuilder);
                    }
                }
            } else if (operatorFactory instanceof CommitterOperatorFactory) {
                Sink sink = ClassUtils.getFiledValue((operatorFactory), "sink", Sink.class);
                switch (sink.getClass().getName()) {
                    case FLINK_CONNECTOR_KAFKA_SINK:
                        new FlinkKafkaSinkEntity(sink).add2Lineage(lineageRequestBuilder);
                }
            } else if (operatorFactory instanceof SimpleInputFormatOperatorFactory) {
                StreamSource source = ClassUtils.getFiledValue((operatorFactory), "operator", StreamSource.class);
                Function userFunction = source.getUserFunction();
                String userFunctionClass = userFunction.getClass().getName();
                if (INPUT_FORMAT_SOURCE_FUNCTION_CLASS.equals(userFunctionClass)) {
                    InputFormat inputFormat = ClassUtils.getFiledValue((userFunction), "format", InputFormat.class);
                    String inputFormatName = inputFormat.getClass().getName();
                    switch (inputFormatName) {
                        case JDBC_ROW_DATA_INPUT_FORMAT_CLASS:
                            new GenericJdbcSourceFunctionEntity(inputFormat).add2Lineage(lineageRequestBuilder);
                    }
                }
            } else if (operatorFactory instanceof AbstractStreamOperatorFactory) {
                SinkOperator sink = ClassUtils.getFiledValue((operatorFactory), "operator", SinkOperator.class);
                Function userFunction = sink.getUserFunction();
                String userFunctionClass = userFunction.getClass().getName();
                switch (userFunctionClass) {
                    case GENERIC_JDBC_SINK_FUNCTION_CLASS:
                        new GenericJdbcSinkFunctionEntity(userFunction).add2Lineage(lineageRequestBuilder);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final String FLINK_KAFKA_CONSUMER_CLASS = "org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer";
    private static final String GENERIC_JDBC_SINK_FUNCTION_CLASS = "org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction";
    private static final String INPUT_FORMAT_SOURCE_FUNCTION_CLASS = "org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction";
    private static final String JDBC_ROW_DATA_INPUT_FORMAT_CLASS = "org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat";

    private static final String FLINK_CONNECTOR_KAFKA_SOURCE = "org.apache.flink.connector.kafka.source.KafkaSource";
    private static final String FLINK_CONNECTOR_KAFKA_SINK = "org.apache.flink.connector.kafka.sink.KafkaSink";
}
