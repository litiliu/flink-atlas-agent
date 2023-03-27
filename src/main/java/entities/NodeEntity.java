package entities;

import com.cisco.webex.datahub.client.lineage.request.LineageRequest.LineageRequestBuilder;

public abstract class NodeEntity<T> {

    protected T node;

    public NodeEntity(Object node) {
        this.node = (T) node;
    }

    public abstract void add2Lineage(LineageRequestBuilder builder);

}
