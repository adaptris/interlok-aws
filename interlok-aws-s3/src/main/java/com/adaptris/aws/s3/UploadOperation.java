package com.adaptris.aws.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.lms.FileBackedMessage;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.ManagedThreadFactory;
import com.adaptris.interlok.InterlokException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.util.IOUtils;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Download an object from S3 using {@link TransferManager}.
 * 
 * @author lchan
 * @config amazon-s3-upload
 */
@AdapterComponent
@ComponentProfile(summary = "Amazon S3 Upload using Transfer Manager")
@XStreamAlias("amazon-s3-upload")
@DisplayOrder(order = {"key", "bucketName",})
public class UploadOperation extends S3OperationImpl {

  private transient ManagedThreadFactory threadFactory = new ManagedThreadFactory();

  public UploadOperation() {

  }

  @Override
  public void execute(AmazonS3Client s3, AdaptrisMessage msg) throws InterlokException {
    TransferManager tm = new TransferManager(s3);
    String bucketName = getBucketName().extract(msg);
    String key = getKey().extract(msg);
    ObjectMetadata s3meta = new ObjectMetadata();
    s3meta.setContentLength(msg.getSize());
    try (InputStream in = msg.getInputStream()) {
      log.debug("Uploading to {} from bucket {}", key, bucketName);
      Upload upload = tm.upload(bucketName, key, in, s3meta);
      Thread t = threadFactory.newThread(new MyProgressListener(upload));
      t.setName(Thread.currentThread().getName());
      t.start();
      upload.waitForCompletion();
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
  }


  private void write(File f, AdaptrisMessage msg) throws IOException {
    if (msg instanceof FileBackedMessage) {
      log.trace("Initialising Message from {}", f.getCanonicalPath());
      ((FileBackedMessage) msg).initialiseFrom(f);
    } else {
      try (FileInputStream in = new FileInputStream(f); OutputStream out = msg.getOutputStream()) {
        IOUtils.copy(in, out);
      }
    }
  }

  private class MyProgressListener implements Runnable {
    private Upload upload;

    MyProgressListener(Upload upload) {
      this.upload = upload;
    }

    public void run() {
      while (!upload.isDone()) {
        log.trace("Uploaded : {}%", (upload.getProgress().getPercentTransferred() / 1));
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
      }
    }
  }

}
