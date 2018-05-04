package com.adaptris.aws.s3;

import static org.apache.commons.lang.StringUtils.isEmpty;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.http.util.Args;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.lms.FileBackedMessage;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.ManagedThreadFactory;
import com.adaptris.interlok.config.DataInputParameter;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.util.IOUtils;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Download an object from S3 using {@link TransferManager}.
 * 
 * @config amazon-transfer-manager-download-service
 * @deprecated since 3.3.0 use {@link DownloadOperation} instead as part of an {@link S3Service}
 */
@AdapterComponent
@ComponentProfile(summary = "Amazon S3 Download Service using Transfer Manager")
@XStreamAlias("amazon-transfer-manager-download-service")
@DisplayOrder(order = {"authentication", "key", "bucketName", "tempDirectory"})
@Deprecated
public class TransferManagerDownload extends S3ServiceImpl {

  @NotNull
  @Valid
  private DataInputParameter<String> bucketName;
  @NotNull
  @Valid
  private DataInputParameter<String> key;

  @AdvancedConfig
  private String tempDirectory;

  private transient File tempDir;
  private transient ManagedThreadFactory threadFactory = new ManagedThreadFactory(this.getClass().getSimpleName());

  public TransferManagerDownload() {

  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    TransferManager tm = new TransferManager(amazonClient());
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
    } finally {
      tm.shutdownNow(false);
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
    super.initService();
    if (!isEmpty(getTempDirectory())) {
      tempDir = new File(getTempDirectory());
    }
  }

  public DataInputParameter<String> getKey() {
    return key;
  }

  public void setKey(DataInputParameter<String> key) {
    this.key = Args.notNull(key, "key");;
  }

  public DataInputParameter<String> getBucketName() {
    return bucketName;
  }


  public void setBucketName(DataInputParameter<String> bucketName) {
    this.bucketName = Args.notNull(bucketName, "bucketName");
  }


  /**
   * @return the tempDirectory
   */
  public String getTempDirectory() {
    return tempDirectory;
  }


  /**
   * Set the temp directory to store files.
   * 
   * @param s the tempDirectory to set, if not specified defaults to {@code java.io.tmpdir}
   */
  public void setTempDirectory(String s) {
    this.tempDirectory = s;
  }

  private class MyProgressListener implements Runnable {
    private Download download;

    MyProgressListener(Download download) {
      this.download = download;
    }

    public void run() {
      while (!download.isDone()) {
        log.trace("Downloaded : {}%", (download.getProgress().getPercentTransferred() / 1));
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
      }
    }
  }
}
