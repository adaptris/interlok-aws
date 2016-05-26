package com.adaptris.aws.s3;

import static org.apache.commons.lang.StringUtils.isEmpty;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.validation.constraints.NotNull;

import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.lms.FileBackedMessage;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.ManagedThreadFactory;
import com.adaptris.interlok.config.DataInputParameter;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.util.IOUtils;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@ComponentProfile(summary = "Amazon S3 Download Service using Transfer Manager")
@XStreamAlias("amazon-transfer-manager-download-service")
public class TransferManagerDownload extends ServiceImp {

  @NotNull
  private DataInputParameter<String> bucketName;
  @NotNull
  private DataInputParameter<String> key;

  private String tempDirectory;
  private transient AmazonS3Client s3;

  private transient File tempDir;
  private transient ManagedThreadFactory threadFactory = new ManagedThreadFactory();

  public TransferManagerDownload() {

  }


  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    TransferManager tm = new TransferManager(s3);
    try {
      GetObjectRequest request = new GetObjectRequest(getBucketName().extract(msg), getKey().extract(msg));
      log.debug("Getting {} from bucket {}", request.getKey(), request.getBucketName());
      File destFile = File.createTempFile(this.getClass().getSimpleName(), "", tempDir);
      Download download = tm.download(request, destFile);
      Thread t = threadFactory.newThread(new MyProgressListener(download));
      t.setName(Thread.currentThread().getName());
      t.start();
      download.waitForCompletion();
      write(destFile, msg);
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    }
  }

  private void write(File f, AdaptrisMessage msg) throws IOException {
    if (msg instanceof FileBackedMessage) {
      ((FileBackedMessage) msg).initialiseFrom(f);
    } else {
      try (FileInputStream in = new FileInputStream(f); OutputStream out = msg.getOutputStream()) {
        IOUtils.copy(in, out);
      }
    }
  }


  @Override
  public void prepare() throws CoreException {}

  @Override
  protected void closeService() {}

  @Override
  protected void initService() throws CoreException {
    s3 = new AmazonS3Client();
    if (!isEmpty(getTempDirectory())) {
      tempDir = new File(getTempDirectory());
    }
  }

  public DataInputParameter<String> getKey() {
    return key;
  }

  public void setKey(DataInputParameter<String> key) {
    this.key = key;
  }

  public DataInputParameter<String> getBucketName() {
    return bucketName;
  }


  public void setBucketName(DataInputParameter<String> bucketName) {
    this.bucketName = bucketName;
  }


  /**
   * @return the tempDirectory
   */
  public String getTempDirectory() {
    return tempDirectory;
  }


  /**
   * @param tempDirectory the tempDirectory to set
   */
  public void setTempDirectory(String tempDirectory) {
    this.tempDirectory = tempDirectory;
  }

  private class MyProgressListener implements Runnable {
    private Download download;

    MyProgressListener(Download download) {
      this.download = download;
    }

    public void run() {
      while (!download.isDone()) {
        log.trace("Percent Complete : {}%", download.getProgress().getPercentTransferred());
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
        }
      }
    }
  }
}
