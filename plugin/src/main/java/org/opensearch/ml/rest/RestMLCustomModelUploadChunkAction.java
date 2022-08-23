/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.rest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.opensearch.client.node.NodeClient;
import org.opensearch.ml.common.transport.custom.upload.MLUploadChunkInput;
import org.opensearch.ml.common.transport.custom.upload.MLUploadModelChunkAction;
import org.opensearch.ml.common.transport.custom.upload.MLUploadModelChunkRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.opensearch.ml.plugin.MachineLearningPlugin.ML_BASE_URI;

public class RestMLCustomModelUploadChunkAction extends BaseRestHandler {
    private static final String ML_UPLOAD_MODEL_CHUNK_ACTION = "ml_upload_model_chunk_action";

    /**
     * Constructor
     */
    public RestMLCustomModelUploadChunkAction() {}

    @Override
    public String getName() {
        return ML_UPLOAD_MODEL_CHUNK_ACTION;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList
                .of(new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s/custom_model/upload_chunk/{%s}/{%s}/{%s}", ML_BASE_URI, "name", "version", "chunk_number")));
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        MLUploadModelChunkRequest mlUploadModelRequest = getRequest(request);
        return channel -> client.execute(MLUploadModelChunkAction.INSTANCE, mlUploadModelRequest, new RestToXContentListener<>(channel));
    }

    /**
     * Creates a MLTrainingTaskRequest from a RestRequest
     *
     * @param request RestRequest
     * @return MLTrainingTaskRequest
     */
    @VisibleForTesting
    MLUploadModelChunkRequest getRequest(RestRequest request) throws IOException {
        String name = request.param("name");
//        System.out.println(name);
        String version = request.param("version");
//        System.out.println(version);
        String chunk_number = request.param("chunk_number");
//        System.out.println(chunk_number);
        byte[] content = request.content().streamInput().readAllBytes();
//        System.out.println(content.length);
        MLUploadChunkInput mlInput = new MLUploadChunkInput(name, Integer.parseInt(version), content, Integer.parseInt(chunk_number));

        return new MLUploadModelChunkRequest(mlInput);
    }
}
