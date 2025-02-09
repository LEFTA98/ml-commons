/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.common.transport.custom;

import lombok.Builder;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLTask;
//import org.opensearch.ml.common.transport.custom.predict.MLPredictModelInput;

import java.io.IOException;
import java.util.Locale;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * ML input data: algirithm name, parameters and input data set.
 */
@Data
@Log4j2
public class MLForwardInput implements ToXContentObject, Writeable {

    public static final String ALGORITHM_FIELD = "algorithm";
    public static final String NAME_FIELD = "name";
    public static final String VERSION_FIELD = "version";
    public static final String TASK_ID_FIELD = "task_id";
    public static final String WORKER_NODE_ID_FIELD = "worker_node_id";
    public static final String REQUEST_TYPE_FIELD = "request_type";
    public static final String ML_TASK_FIELD = "ml_task";
    public static final String URL_FIELD = "url";
    public static final String CHUNK_NUMBER_FIELD = "chunk_number";
    public static final String PREDICT_INPUT_FIELD = "predict_input";

    // Algorithm name
    private FunctionName algorithm = FunctionName.CUSTOM;

    private String name;
    private Integer version;
    private String taskId;
    private String workerNodeId;
    private MLForwardRequestType requestType;
    private MLTask mlTask;
    private byte[] url;
    private Integer chunkNumber;
//    MLPredictModelInput predictModelInput;

    @Builder(toBuilder = true)
    public MLForwardInput(String name, Integer version, String taskId, String workerNodeId, MLForwardRequestType requestType, MLTask mlTask, byte[] url, Integer chunkNumber) {
        this.name = name;
        this.version = version;
        this.taskId = taskId;
        this.workerNodeId = workerNodeId;
        this.requestType = requestType;
        this.mlTask = mlTask;
        this.url = url;
        this.chunkNumber = chunkNumber;
//        this.predictModelInput = predictModelInput;
    }


    public MLForwardInput(StreamInput in) throws IOException {
        this.name = in.readOptionalString();
        this.version = in.readOptionalInt();
        this.algorithm = in.readEnum(FunctionName.class);
        this.taskId = in.readOptionalString();
        this.workerNodeId = in.readOptionalString();
        this.requestType = in.readEnum(MLForwardRequestType.class);
        this.url = in.readByteArray();
        this.chunkNumber = in.readOptionalInt();
        if (in.readBoolean()) {
            mlTask = new MLTask(in);
        }
//        if (in.readBoolean()) {
//            this.predictModelInput = new MLPredictModelInput(in);
//        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(name);
        out.writeOptionalInt(version);
        out.writeEnum(algorithm);
        out.writeOptionalString(taskId);
        out.writeOptionalString(workerNodeId);
        out.writeEnum(requestType);
        if (this.url != null) {
            out.writeByteArray(url);
        }
        out.writeOptionalInt(chunkNumber);
        if (this.mlTask != null) {
            out.writeBoolean(true);
            mlTask.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
//        if (predictModelInput != null) {
//            out.writeBoolean(true);
//            predictModelInput.writeTo(out);
//        } else {
//            out.writeBoolean(false);
//        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ALGORITHM_FIELD, algorithm.name());
        builder.field(NAME_FIELD, name);
        builder.field(VERSION_FIELD, version);
        builder.field(TASK_ID_FIELD, taskId);
        builder.field(WORKER_NODE_ID_FIELD, workerNodeId);
        builder.field(REQUEST_TYPE_FIELD, requestType);
        if (mlTask != null) {
            mlTask.toXContent(builder, params);
        }
        if (url != null) {
            builder.field(URL_FIELD, url);
        }
        if (chunkNumber != null) {
            builder.field(CHUNK_NUMBER_FIELD, chunkNumber);
        }
//        if (predictModelInput != null) {
//            predictModelInput.toXContent(builder, params);
//        }
        builder.endObject();
        return builder;
    }

    public static MLForwardInput parse(XContentParser parser) throws IOException {
        String algorithmName = null;
        String name = null;
        Integer version = null;
        String taskId = null;
        String workerNodeId = null;
        MLForwardRequestType requestType = null;
        MLTask mlTask = null;
        byte[] url = null;
        Integer chunkNumber = null;
//        MLPredictModelInput predictModelInput = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case ALGORITHM_FIELD:
                    algorithmName = parser.text().toUpperCase(Locale.ROOT);
                    break;
                case NAME_FIELD:
                    name = parser.text();
                    break;
                case VERSION_FIELD:
                    version = parser.intValue();
                    break;
                case TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                case WORKER_NODE_ID_FIELD:
                    workerNodeId = parser.text();
                    break;
                case REQUEST_TYPE_FIELD:
                    requestType = MLForwardRequestType.valueOf(parser.text().toUpperCase(Locale.ROOT));
                    break;
                case ML_TASK_FIELD:
                    mlTask = MLTask.parse(parser);
                    break;
                case URL_FIELD:
                    url = parser.binaryValue();
                    break;
                case CHUNK_NUMBER_FIELD:
                    chunkNumber = parser.intValue();
                    break;
//                case PREDICT_INPUT_FIELD:
//                    predictModelInput = MLPredictModelInput.parse(parser);
//                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new MLForwardInput(name, version, taskId, workerNodeId, requestType, mlTask, url, chunkNumber);
    }


    public FunctionName getFunctionName() {
        return this.algorithm;
    }

}
