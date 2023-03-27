import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.alibaba.fastjson.JSON;

public class TestMain {

    static String kafka2kafkanote = "\n"
        + "\n"
        + "\n"
        + "CREATE TABLE orders_src (\n"
        + "  order_number BIGINT,\n"
        + "  price        DECIMAL(32,2),\n"
        + "  address        STRING,\n"
        + "  buyer        ROW<first_name STRING, last_name STRING>,\n"
        + "  order_time   TIMESTAMP(3)\n"
        + ") WITH (\n"
        + "   'connector' = 'kafka',\n"
        + "   'topic' = 'flink-test-1',\n"
        + "   'properties.bootstrap.servers' = 'bt1-kafka-s.webex.com:9092',\n"
        + "   'properties.group.id' = 'csv',\n"
        + "   'format' = 'json'\n"
        + ");\n"
        + "\n"
        + "CREATE TABLE orders_dest (\n"
        + "  order_number BIGINT,\n"
        + "  price        DECIMAL(32,2),\n"
        + "  address        STRING,\n"
        + "  buyer        ROW<first_name STRING, last_name STRING>,\n"
        + "  order_time   TIMESTAMP(3)\n"
        + ") WITH (\n"
        + "   'connector' = 'kafka',\n"
        + "   'topic' = 'flink-test-2',\n"
        + "   'properties.bootstrap.servers' = 'bt1-kafka-s.webex.com:9092',\n"
        + "   'properties.group.id' = 'csv',\n"
        + "   'format' = 'json'\n"
        + ");\n"
        + "\n"
        + "insert into orders_dest select order_number, price, address, buyer,  order_time from orders_src;\n"
        + "\n";

    static String kafka2jdbcnote = "\n"
        + "\n"
        + "\n"
        + "CREATE TABLE orders_src (\n"
        + "  order_number BIGINT,\n"
        + "  address        STRING,\n"
        + "  order_time   TIMESTAMP(3)\n"
        + ") WITH (\n"
        + "   'connector' = 'kafka',\n"
        + "   'topic' = 'flink-test-1',\n"
        + "   'properties.bootstrap.servers' = 'bt1-kafka-s.webex.com:9092',\n"
        + "   'properties.group.id' = 'csv',\n"
        + "   'format' = 'json'\n"
        + ");\n"
        + "\n"
        + "CREATE TABLE orders_dest (\n"
        + "  order_number BIGINT,\n"
        + "  address        STRING,\n"
        + "  order_time   TIMESTAMP(3)\n"
        + ") WITH (\n"
        + "   'connector' = 'jdbc',\n"
        + "   'url' = 'jdbc:mysql://tidb-lab.webex.com:4000/test',\n"
        + "   'table-name' = 'orders_dest',\n"
        + "   'username' = 'udp',\n"
        + "   'password' = 'K$6Uu8U$JlcJWXeT'\n"
        + ");\n"
        + "\n"
        + "insert into orders_dest select order_number,  address,   order_time from orders_src;\n"
        + "\n";;


    static String jdbc2jdbcnote = "\n"
        + "\n"
        + "\n"
        + "CREATE TABLE orders_src (\n"
        + "  order_number BIGINT,\n"
        + "  address        STRING\n"
        + ") WITH (\n"
        + "   'connector' = 'jdbc',\n"
        + "   'url' = 'jdbc:mysql://tidb-lab.webex.com:4000/test',\n"
        + "   'table-name' = 'orders_src',\n"
        + "   'username' = 'udp',\n"
        + "   'password' = 'K$6Uu8U$JlcJWXeT'\n"
        + ");\n"
        + "\n"
        + "CREATE TABLE orders_dest (\n"
        + "  order_number BIGINT,\n"
        + "  address        STRING\n"
        + ") WITH (\n"
        + "   'connector' = 'jdbc',\n"
        + "   'url' = 'jdbc:mysql://tidb-lab.webex.com:4000/test',\n"
        + "   'table-name' = 'orders_dest',\n"
        + "   'username' = 'udp',\n"
        + "   'password' = 'K$6Uu8U$JlcJWXeT'\n"
        + ");\n"
        + "\n"
        + "insert into orders_dest select order_number,  address from orders_src;\n"
        + "\n";;


    public static void main(String[] args) throws Exception {
        FlinkSqlParamWrapper task = new FlinkSqlParamWrapper();
        task.setNote(kafka2kafkanote);
//        task.setNote(kafka2jdbcnote);
//        task.setNote(jdbc2jdbcnote);
        Map<String, String> map = new HashMap<>();
        map.put("parallelism.default", "1");
        map.put("pipeline.name", "test_flink_sql");
        map.put("datahub.url", "www.datahub.com");
//        map.put("state.checkpoints.dir", "hdfs:/user/pda/chk/6188408820928/flink/sql/");
        task.setFlinkProperties(JSON.toJSONString(map));
        System.out.println(JSON.toJSONString(task));
        CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            try {
                Main.main(new String[] {JSON.toJSONString(task)});
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        t.start();
        latch.await();
    }


}
