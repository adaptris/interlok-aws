package com.adaptris.aws.s3;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.validation.Valid;

import org.apache.commons.lang.StringUtils;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.aws.s3.meta.S3ObjectMetadata;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.ManagedThreadFactory;
import com.adaptris.interlok.InterlokException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Upload an object to S3 using {@link TransferManager}.
 * 
 * @author lchan
 * @config amazon-s3-upload
 */
@AdapterComponent
@ComponentProfile(summary = "Amazon S3 Upload using Transfer Manager")
@XStreamAlias("amazon-s3-upload")
@DisplayOrder(order = {"bucketName", "key", "userMetadataFilter", "objectMetadata"})
public class UploadOperation extends TransferOperation {

  private transient ManagedThreadFactory threadFactory = new ManagedThreadFactory();

  @AdvancedConfig
  @Valid
  private List<S3ObjectMetadata> objectMetadata = new ArrayList<>();
  
  @Override
  public void execute(AmazonS3Client s3, AdaptrisMessage msg) throws InterlokException {
    TransferManager tm = transferManager(s3);
    String bucketName = getBucketName().extract(msg);
    String key = getKey().extract(msg);
    ObjectMetadata s3meta = new ObjectMetadata();
    s3meta.setContentLength(msg.getSize());
    if(StringUtils.isNotEmpty(msg.getContentEncoding())) {
      s3meta.setContentEncoding(msg.getContentEncoding());
    }
    s3meta.setUserMetadata(filterMetadata(msg));
    for(S3ObjectMetadata m: getObjectMetadata()) {
      m.apply(msg, s3meta);
    }
    try (InputStream in = msg.getInputStream()) {
      log.debug("Uploading to {} in bucket {}", key, bucketName);
      Upload upload = tm.upload(bucketName, key, in, s3meta);
      threadFactory.newThread(new MyProgressListener(Thread.currentThread().getName(), upload)).start();
      upload.waitForCompletion();
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
    finally {
      tm.shutdownNow(false);
    }
  }
  
  public List<S3ObjectMetadata> getObjectMetadata() {
    return objectMetadata;
  }

  public void setObjectMetadata(List<S3ObjectMetadata> objectMetadata) {
    this.objectMetadata = objectMetadata;
  }

  private class MyProgressListener implements Runnable {
    private Upload upload;
    private String name;

    MyProgressListener(String name, Upload upload) {
      this.upload = upload;
      this.name = name;
    }

    public void run() {
      Thread.currentThread().setName(name);
      while (!upload.isDone()) {
        log.trace("Uploaded : {}%", (upload.getProgress().getPercentTransferred() / 1));
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }
}
