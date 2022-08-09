package org.opensearch.ml.engine.algorithms.custom;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.FileUtils;
import org.opensearch.action.ActionListener;

import java.io.*;
@Log4j2
public class CustomModelManager {
    public static final String DJL_CUSTOM_MODELS_PATH = "/djl/custom_models/";
    private static final int CHUNK_SIZE = 10_000_000; // 10MB

    public void readDownloadedChunk (String modelName, Integer version, byte[] url, Integer chunkNumber, ActionListener<String> listener) throws IOException  {
        // current MLUploadInput class has main content in the form of url parameter - need to change so that url will be the actual text content of our compressed torchscript file in bytes
        // this can take the form of a String (in which case buffer stuff is nonsense) or a File that we will need to read contents

        InputStream inStream = null;
        String destFileName;
        String outputPath = DJL_CUSTOM_MODELS_PATH + "upload/" + version + "/" + modelName + "/chunks/";

        try {
            inStream = new BufferedInputStream(new ByteArrayInputStream(url));

            byte[] temp = null;
            String chunkName = chunkNumber + "";
            destFileName = outputPath + chunkName;

            inStream.read(temp, 0, CHUNK_SIZE);
            write(temp, destFileName);
            listener.onResponse(destFileName);
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            inStream.close();
        }
    }

    public void write(byte[] DataByteArray, String destinationFileName) {
        try {
            File file = new File(destinationFileName);
            FileUtils.createParentDirectories(file);
//            OutputStream output = null;
            try (OutputStream output = new BufferedOutputStream(new FileOutputStream(destinationFileName))){

//                output = new BufferedOutputStream(new FileOutputStream(destinationFileName));
                output.write(DataByteArray);
                System.out.println("Writing Process Was Performed");
            } catch (IOException e) {
                e.printStackTrace();
            }
//            finally {
//                output.close();
//            }
        } catch (FileNotFoundException ex) {
            log.error("File not found", ex);
        } catch (IOException ex) {
            log.error("Fail to write bytes to file", ex);
        }
    }
    public void write(byte[] DataByteArray, File destinationFile, boolean append) {
        try {
            OutputStream output = null;
            try {
//                File file = new File(destinationFileName);
                FileUtils.createParentDirectories(destinationFile);
                output = new BufferedOutputStream(new FileOutputStream(destinationFile, append));
                output.write(DataByteArray);
                System.out.println("Writing Process Was Performed");
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                output.close();
            }
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
