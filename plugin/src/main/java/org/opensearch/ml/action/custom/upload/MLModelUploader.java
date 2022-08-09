package org.opensearch.ml.action.custom.upload;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.opensearch.action.ActionListener;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.Model;
import org.opensearch.ml.common.transport.custom.upload.MLUploadInput;
import org.opensearch.ml.engine.algorithms.custom.CustomModelManager;
import org.opensearch.ml.indices.MLIndicesHandler;
import org.opensearch.ml.task.MLTaskManager;
import org.opensearch.threadpool.ThreadPool;

import java.io.File;
import java.security.PrivilegedActionException;
import java.util.Base64;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.ml.common.CommonValue.ML_MODEL_INDEX;
import static org.opensearch.ml.plugin.MachineLearningPlugin.TASK_THREAD_POOL;

@Log4j2
public class MLModelUploader {

    public static final int TIMEOUT_IN_MILLIS = 5000;
    private final CustomModelManager customModelManager;
    private final MLIndicesHandler mlIndicesHandler;
    private final MLTaskManager mlTaskManager;
    private final ThreadPool threadPool;
    private final Client client;

    public MLModelUploader(CustomModelManager customModelManager, MLIndicesHandler mlIndicesHandler, MLTaskManager mlTaskManager, ThreadPool threadPool, Client client) {
        this.customModelManager = customModelManager;
        this.mlIndicesHandler = mlIndicesHandler;
        this.mlTaskManager = mlTaskManager;
        this.threadPool = threadPool;
        this.client = client;
    }

    public void uploadModel(MLUploadInput mlUploadInput, MLTask mlTask) {
        Semaphore semaphore = new Semaphore(1);
        String taskId = mlTask.getTaskId();
        mlTaskManager.add(mlTask);

        AtomicInteger uploaded = new AtomicInteger(0);
        threadPool.executor(TASK_THREAD_POOL).execute(() -> {
            try {
                String modelName = mlUploadInput.getName(); // get name of model
                Integer version = mlUploadInput.getVersion(); // get version of model
                String returnedString = customModelManager.readDownloadedChunk(modelName, version, mlUploadInput.getUrl(), ActionListener.wrap(toReturn -> {
                    mlIndicesHandler.initModelIndexIfAbsent(ActionListener.wrap(res -> {
                        System.out.println(Thread.currentThread().getName());
                        semaphore.acquire();//TODO: check which thread are using, this will block thread
                        File file = new File(toReturn);
                        byte[] bytes = Files.toByteArray(file);
                        Model model = new Model();
                        model.setName(FunctionName.KMEANS.name());
                        model.setVersion(1);
                        model.setContent(bytes);
                        int chunkNum = Integer.parseInt(file.getName());
                        MLModel mlModel = MLModel.builder()
                                .name(modelName)
                                .algorithm(FunctionName.CUSTOM)
                                .version(version)
                                .chunkNumber(chunkNum)
                                .totalChunks(nameList.size())
                                .content(Base64.getEncoder().encodeToString(bytes))
                                .build();
                    }));
                        }
                ));
            } catch (PrivilegedActionException e) {}
        });
    }

}