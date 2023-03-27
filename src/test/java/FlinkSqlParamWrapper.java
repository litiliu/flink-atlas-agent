import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FlinkSqlParamWrapper {

    private boolean statementSet;

    private boolean batchModel;

    private String note;

    private String flinkProperties;

    private String jobName;


    public boolean isStatementSet() {
        return statementSet;
    }

    public void setStatementSet(boolean statementSet) {
        this.statementSet = statementSet;
    }

    public boolean isBatchModel() {
        return batchModel;
    }

    public void setBatchModel(boolean batchModel) {
        this.batchModel = batchModel;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public String getFlinkProperties() {
        return flinkProperties;
    }

    public void setFlinkProperties(String flinkProperties) {
        this.flinkProperties = flinkProperties;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

}
