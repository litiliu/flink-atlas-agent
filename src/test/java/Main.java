import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);


    public static void main(String[] args) throws Exception {
        FlinkSqlParamWrapper wrapper = JSON.parseObject(args[0], FlinkSqlParamWrapper.class);
        Map<String, String> config = JSON.parseObject(wrapper.getFlinkProperties(), Map.class);

        Configuration configuration = new Configuration();
        if (config != null && !config.isEmpty()) {
            configuration = Configuration.fromMap(config);
        }
        boolean useStatementSet = Boolean.TRUE;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<StatementParam> ddl = new ArrayList<>();
        List<StatementParam> trans = new ArrayList<>();
        List<StatementParam> execute = new ArrayList<>();
        loadUdfJar(wrapper.getFlinkProperties());

        String[] statements = FlinkInterceptor.splitStatement(wrapper.getNote());
        //separate
        for (String item : statements) {
            String statement = FlinkInterceptor.prepareStatement(item);
            if (statement.isEmpty()) {
                continue;
            }
            SqlType operationType = Operations.getOperationType(statement);
            if (operationType.equals(SqlType.INSERT) || operationType.equals(SqlType.SELECT)) {
                trans.add(new StatementParam(statement, operationType));
                if (!useStatementSet) {
                    break;
                }
            } else if (operationType.equals(SqlType.EXECUTE)) {
                execute.add(new StatementParam(statement, operationType));
                if (!useStatementSet) {
                    break;
                }
            } else {
                ddl.add(new StatementParam(statement, operationType));
            }
        }
        //execute
        for (StatementParam item : ddl) {
            String sql = item.getValue();
            logger.warn(String.format("executing sql : %s", item.getValue()));
            tEnv.executeSql(item.getValue()).print();
            logger.warn(String.format("finish executing: %s", sql));
        }
        if (trans.size() > 0) {
            if (useStatementSet) {
                List<String> inserts = new ArrayList<>();
                for (StatementParam item : trans) {
                    if (item.getType().equals(SqlType.INSERT)) {
                        inserts.add(item.getValue());
                    }
                }
                logger.warn("executing FlinkSQL statementSet: " + String.join(Constants.SQL_SEPARATOR, inserts));
                StatementSet statementSet = tEnv.createStatementSet();
                for (String sql : inserts) {
                    statementSet.addInsertSql(sql);
                }
                statementSet.execute();
                logger.warn("execute finish");
            } else {
                for (StatementParam item : trans) {
                    String sql = item.getValue();
                    logger.warn(String.format("executing FlinkSQL: ", sql));
                    tEnv.executeSql(item.getValue());
                    logger.warn(String.format("finish executing:", sql));
                    break;
                }
            }
        }
        if (execute.size() > 0) {
            List<String> executes = new ArrayList<>();
            for (StatementParam item : execute) {
                executes.add(item.getValue());
                tEnv.executeSql(item.getValue());
                if (useStatementSet) {
                    break;
                }
            }
            logger.warn("executing FlinkSQL statementSet: " + String.join(Constants.SQL_SEPARATOR, executes));
            try {
                env.execute(wrapper.getJobName());
            } catch (Exception e) {
                e.printStackTrace();
            }
            logger.warn("execute finish");
        }
        logger.warn(LocalDateTime.now() + " job submit successfully");
    }

    /**
     * note this method currently only work well with java 8 see https://stackoverflow.com/questions/46694600/java-9-compatability-issue-with-classloader-getsystemclassloader add udf jar to classPath,
     * because the flink planner will use URLClassLoader to load udf class dynamically
     *
     * @param flinkProperties
     * @throws Exception
     */
    private static void loadUdfJar(String flinkProperties) throws Exception {
        if (StringUtils.isNotEmpty(flinkProperties)) {
            Map<String, String> propertiesMap = JSON.parseObject(flinkProperties, Map.class);
            String jarUriStr = propertiesMap.get("pipeline.jars");
            if (StringUtils.isNotEmpty(jarUriStr)) {
                String[] udfPaths = jarUriStr.split(Constants.COMMA);
                if (udfPaths != null) {
                    //try to access the protected method: addURL
                    Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                    method.setAccessible(true);

                    URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
                    /**
                     * udfPath: file:///tmp/file.jar
                     */
                    for (String udfPath : udfPaths) {
                        File file = new File(udfPath.replace(Constants.protocal, ""));
                        method.invoke(urlClassLoader, file.toURI().toURL());
                    }
                }
            }
        }
    }

}
