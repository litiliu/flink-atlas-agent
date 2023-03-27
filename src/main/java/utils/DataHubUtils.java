package utils;

import org.apache.commons.lang3.StringUtils;

public class DataHubUtils {

    public static String getCluster(String bootstrapServer) {
        if (StringUtils.isNotEmpty(bootstrapServer)) {
            int index = bootstrapServer.indexOf("-");
            if (index > -1) {
                return bootstrapServer.substring(0, index);
            }
        }
        return null;
    }
}
