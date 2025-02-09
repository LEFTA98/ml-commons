package org.opensearch.ml.common.transport.custom.load;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class LoadModelResponse extends ActionResponse implements ToXContentObject {
    public static final String TASK_ID_FIELD = "task_id";
    public static final String STATUS_FIELD = "status";

    private String taskId;
    private String status;

    public LoadModelResponse(StreamInput in) throws IOException {
        super(in);
        this.taskId = in.readString();
        this.status = in.readString();
    }

    public LoadModelResponse(String taskId, String status) {
        this.taskId = taskId;
        this.status= status;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(taskId);
        out.writeString(status);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TASK_ID_FIELD, taskId);
        builder.field(STATUS_FIELD, status);
        builder.endObject();
        return builder;
    }
}
