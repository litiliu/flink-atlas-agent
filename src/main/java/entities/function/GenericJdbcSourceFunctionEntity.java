package entities.function;


import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.table.JdbcRowDataInputFormat;

import entities.NodeEntity;
import utils.ClassUtils;

import com.cisco.webex.datahub.client.lineage.request.LineageRequest.LineageRequestBuilder;

public class GenericJdbcSourceFunctionEntity extends NodeEntity<JdbcRowDataInputFormat> {

    public GenericJdbcSourceFunctionEntity(Object node) {
        super(node);
    }

    @Override
    public void add2Lineage(LineageRequestBuilder builder) {
        JdbcRowDataInputFormat jdbcSinkFunction = this.node;

        try {

            JdbcOutputFormat outputFormat = ClassUtils.getFiledValue(jdbcSinkFunction,
                    "outputFormat", JdbcOutputFormat.class);

            JdbcDmlOptions dmlOptions = null;
            try {
                dmlOptions = ClassUtils.getFiledValue(outputFormat, "dmlOptions", JdbcDmlOptions.class);
            } catch (Throwable t) {
                // do noting
            }

            JdbcConnectionProvider connectionProvider = ClassUtils.getFiledValue(outputFormat,
                    "connectionProvider", JdbcConnectionProvider.class);

            JdbcConnectionOptions jdbcOptions = null;
            if (connectionProvider instanceof SimpleJdbcConnectionProvider) {
                jdbcOptions = ClassUtils.getFiledValue(connectionProvider, "jdbcOptions", JdbcConnectionOptions.class);
            }

            // TODO

        } catch (Throwable t) {
            // TODO
        }

    }
}
