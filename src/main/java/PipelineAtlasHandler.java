import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.cisco.webex.datahub.client.DataHubClientConfig;
import com.cisco.webex.datahub.client.DataHubClientConfig.Env;
import com.cisco.webex.datahub.client.DataHubRestClient;
import com.cisco.webex.datahub.client.lineage.request.JobType;
import com.cisco.webex.datahub.client.lineage.request.LineageOwnerType;
import com.cisco.webex.datahub.client.lineage.request.LineageRequest;

import entities.StreamNodeEntity;

public class PipelineAtlasHandler {

    public static void handle(Pipeline pipeline) {
        try {
            if (pipeline instanceof StreamGraph) {
                handle((StreamGraph) pipeline);
            } else if (pipeline instanceof Plan) {
                handle((Plan) pipeline);
            }
        } catch (Exception e) {
            /**
             *  the exception should be captured,
             *  otherwise lineage problem may cause flink job process crash!
              */
            e.printStackTrace();
        }
    }

    /**
     * integrate with dataHub
     * <p>
     * https://confluence-eng-gpk2.cisco.com/conf/display/WBXCE/DataHub+Client+Library
     */
    private static void handle(StreamGraph streamGraph) throws Exception {
        // flink application entity
        LineageRequest.LineageRequestBuilder builder = new LineageRequest.LineageRequestBuilder();
        // build basic info

        //project name+ jobname
        String jobname = System.getenv("JOB_NAME");
        String nameSpace = System.getenv("NAME_SPACE");
        String jobOwner = System.getenv("JOB_OWNER");
        builder.
            withJobType(JobType.flink).
            withJobName(String.format("%s.%s", nameSpace, jobname)).
            appendOwner(jobOwner, LineageOwnerType.DATAOWNER);

        // add source info
        getSourceStreamNodes(streamGraph).stream().forEach(streamNode -> {
            new StreamNodeEntity(streamNode).add2Lineage(builder);
        });

        /**
         *  add process task
         * Only support [number, letter, underscore, dash]
          */
        builder.successorTask(jobname);
        // add sink info
        getSinkStreamNodes(streamGraph)
            .stream().forEach(streamNode -> {
                new StreamNodeEntity(streamNode).add2Lineage(builder);
            });

        String ciToken = System.getProperty("ci.token",
            "eyJhbGciOiJSUzI1NiJ9.eyJtYWNoaW5lX3R5cGUiOiJib3QiLCJjbHVzdGVyIjoiUEY4NCIsInByaXZhdGUiOiJleUpqZEhraU9pSktWMVFpTENKbGJtTWlPaUpCTVRJNFEwSkRMVWhUTWpVMklpd2lZV3huSWpvaVpHbHlJbjAuLlhfbW52TUNxOEJPTTdldzlGNmxlQmcuanEyaEFjWU8xT0JPUHkwSEE5Qmw1VVRqdUV4VjJLLVVYbHdRNXMtS1pDUG5maV9halF1VC1CbGVpVlBqaWU2MlVWamdVN01Ra1Zrckxqak9yR01uSlc0aVRQU2pFU3JvTDJEU3hBRnEwMWJ4OG9MVlZLSHVpTGZtdVlSalJKcjR6eGFFeVd6cEszOUxFZnVzb01fVTE4QXJUeFlrVVdjb3BNWVYta29qMTk5bVB2Q1kzdkItRlN0eHRtZUNrUDdlLmlrMVBvVGt4dEVRQUVIdW55cjI5ZUEiLCJyZWZlcmVuY2VfaWQiOiJjZDk1M2VjMi0xOThjLTRjNjItOTA2Ny0zM2JiMDYwZWY3MTQiLCJpc3MiOiJodHRwczpcL1wvaWRicm9rZXIud2ViZXguY29tXC9pZGIiLCJ0b2tlbl90eXBlIjoiQmVhcmVyIiwiY2xpZW50X2lkIjoiQzE5MWYxZmM5NWM5MmEwNTE2YTkwMDAzZjU0YTY2OGYxZTViYWM5MGIzZWQyNTdkM2U1ZWRiYzZiOTI2ZTNlZTQiLCJ1c2VyX3R5cGUiOiJtYWNoaW5lIiwidG9rZW5faWQiOiJBYVozcjBORGt5Tmpjek9XUXRORE0xTlMwMFpEZG1MVGcxT0dZdE1UazJZekF4WXpFNE1UaGtOR1V3TXpBek5tWXROVGRoIiwidXNlcl9tb2RpZnlfdGltZXN0YW1wIjoiMjAyMzAzMjQwMTQwMzEuNDc4WiIsInJlYWxtIjoiYzZhMDMzNjktNmYzYy00YmJmLTliYzktYzdhNzNmZDkxOGI1IiwiY2lzX3V1aWQiOiIzZjQyMzYzZC01YjZkLTQxYjQtODQxNS1iYjgwM2VkMTVmN2YiLCJleHBpcnlfdGltZSI6MTY3OTY4OTA4ODYyMX0.UE_35b6Oqy8S7ETOReA09cuAbqWoExdNveRofK3noXuaKYKxKujBymXQuMvZIlaxnY4eC5bdv_mux1FpnNqF6ntcH_Q0u5uEbzKLccG3_962vKZOMMFqMdFxuo0goZ6tFg6dhJnV4INKnRgbIm8FZMYsn-plI8kxmSim28zkUD0Gx-gAgv293SUU6opIMqAZwi3pjlT9d45w5n9lVDlSBQXbZpWZ7i8z00iZuQYZwIcx5-BNen3svpcTWPaCRqWsdRu4JnMRZiqqzA7_lUxJAChtzgDDJnEYFGZan1nnTFluja85xCYQmrBRtFoO1S8fjFIQPh0eDmPPTQ9ghQYTAQ");
        // send to dataHub
        DataHubClientConfig conf = DataHubClientConfig.builder()
            .env(Env.prod)
            .token(ciToken)
            .build();
        System.out.println(builder.build().toString());
        DataHubRestClient.syncUpload(conf, builder.build());

    }

    private static void handle(Plan plan) {
        // TODO
    }


    public static List<StreamNode> getSourceStreamNodes(StreamGraph streamGraph) {
        if (Objects.isNull(streamGraph)) {
            return new ArrayList<>();
        }
        return Optional.ofNullable(streamGraph.getStreamNodes()).orElse(new ArrayList<>()).stream()
            .filter(Objects::nonNull)
            .filter(streamNode -> CollectionUtils.isEmpty(streamNode.getInEdges()))
            .collect(Collectors.toList());
    }

    public static List<StreamNode> getSinkStreamNodes(StreamGraph streamGraph) {
        if (Objects.isNull(streamGraph)) {
            return new ArrayList<>();
        }
        return Optional.ofNullable(streamGraph.getStreamNodes()).orElse(new ArrayList<>()).stream()
            .filter(Objects::nonNull)
            .filter(streamNode -> CollectionUtils.isEmpty(streamNode.getOutEdges()))
            .collect(Collectors.toList());
    }

}
