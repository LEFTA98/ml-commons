/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.common.transport.custom.upload;

import lombok.Builder;
import lombok.Data;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.ml.common.FunctionName;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * ML input data: algorithm name, parameters and input data set.
 */
@Data
public class MLUploadChunkInput implements ToXContentObject, Writeable {

    public static final String ALGORITHM_FIELD = "algorithm";
    public static final String NAME_FIELD = "name";
    public static final String VERSION_FIELD = "version";
    public static final String CONTENT_FIELD = "url";
    public static final String CHUNK_NUMBER_FIELD = "chunk_number";

    // Algorithm name
    private FunctionName algorithm = FunctionName.CUSTOM;

    private String name;
    private Integer version;
    private byte[] url;
    private Integer chunkNumber;

    @Builder(toBuilder = true)
    public MLUploadChunkInput(String name, Integer version, byte[] url, Integer chunkNumber) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(version);
        Objects.requireNonNull(url);
        Objects.requireNonNull(chunkNumber);
        this.name = name;
        this.version = version;
        this.url = url;
        this.chunkNumber = chunkNumber;
        this.algorithm = FunctionName.CUSTOM;
    }


    public MLUploadChunkInput(StreamInput in) throws IOException {
        this.algorithm = in.readEnum(FunctionName.class);
        this.name = in.readString();
        this.version = in.readInt();
        this.chunkNumber = in.readInt();
        boolean uploadModel = in.readBoolean();
        if (uploadModel) {
            this.url = in.readByteArray();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(algorithm);
        out.writeString(name);
        out.writeInt(version);
        out.writeInt(chunkNumber);
        if (url == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByteArray(url);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ALGORITHM_FIELD, algorithm.name());
        builder.field(NAME_FIELD, name);
        builder.field(VERSION_FIELD, version);
        builder.field(CHUNK_NUMBER_FIELD, chunkNumber);
        builder.field(CONTENT_FIELD, url);
        builder.endObject();
        return builder;
    }

    public static MLUploadChunkInput parse(XContentParser parser, byte[] content) throws IOException {
        String name = null;
        Integer version = null;
        Integer chunkNumber = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case NAME_FIELD:
                    name = parser.text();
                    break;
                case VERSION_FIELD:
                    version = parser.intValue();
                    break;
                case CHUNK_NUMBER_FIELD:
                    chunkNumber = parser.intValue();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new MLUploadChunkInput(name, version, content, chunkNumber);
    }


    public FunctionName getFunctionName() {
        return this.algorithm;
    }

}
