/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.rest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.ml.common.transport.custom.upload.MLUploadInput;
import org.opensearch.ml.common.transport.custom.upload.MLUploadModelAction;
import org.opensearch.ml.common.transport.custom.upload.MLUploadModelRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.ml.plugin.MachineLearningPlugin.ML_BASE_URI;

public class RestMLCustomModelUploadAction extends BaseRestHandler {
    private static final String ML_UPLOAD_MODEL_ACTION = "ml_upload_model_action";

    /**
     * Constructor
     */
    public RestMLCustomModelUploadAction() {}

    @Override
    public String getName() {
        return ML_UPLOAD_MODEL_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
                .of(new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s/custom_model/upload/%s/%s/%s", ML_BASE_URI, "name", "version", "chunk_number")));
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        MLUploadModelRequest mlUploadModelRequest = getRequest(request);
        return channel -> client.execute(MLUploadModelAction.INSTANCE, mlUploadModelRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Creates a MLTrainingTaskRequest from a RestRequest
     *
     * @param request RestRequest
     * @return MLTrainingTaskRequest
     */
    @VisibleForTesting
    MLUploadModelRequest getRequest(RestRequest request) throws IOException {
//        XContentParser parser = request.contentParser();
        String name = request.param("name");
        System.out.println(name);
        String version = request.param("version");
        System.out.println(version);
        String chunk_number = request.param("chunk_number");
        System.out.println(chunk_number);
        byte[] content = request.content().streamInput().readAllBytes();
        System.out.println(content.length);
//        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
//        MLUploadInput mlInput = MLUploadInput.parse(parser, content);
        MLUploadInput mlInput = new MLUploadInput(name, Integer.parseInt(version), content, Integer.parseInt(chunk_number));

        return new MLUploadModelRequest(mlInput);
    }
}
