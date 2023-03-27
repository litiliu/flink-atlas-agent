import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class JCatalog {

    public static void main(String[] args) {
        StreamExecutionEnvironment flinkExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(flinkExecutionEnv);
        String name = "my_catalog";
        String defaultDatabase = "videomesh";
        String username = "vm_cr_tsdb_ro";
        String password = "7urZUQw!";
        String baseUrl = "jdbc:postgresql://vmn-ch-timescale-bts.webex.com:5432";

        JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);
        tableEnv.registerCatalog("my_catalog", catalog);
        System.out.println("registered catalog");
    }

}
