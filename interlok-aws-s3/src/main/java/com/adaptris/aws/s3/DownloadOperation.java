/*
    Copyright 2018 Adaptris

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.adaptris.aws.s3;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.commons.io.FileCleaningTracker;
import org.apache.commons.io.FileDeleteStrategy;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.lms.FileBackedMessage;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ManagedThreadFactory;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.util.IOUtils;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

/**
 * Download an object from S3 using {@link TransferManager}.
 * 
 * @author lchan
 * @config amazon-s3-download
 */
@AdapterComponent
@ComponentProfile(summary = "Amazon S3 Download using Transfer Manager")
@XStreamAlias("amazon-s3-download")
@DisplayOrder(order = {"bucket", "objectName", "bucketName","key","tempDirectory", "userMetadataFilter"})
@NoArgsConstructor
public class DownloadOperation extends TransferOperation {

  @AdvancedConfig
  private String tempDirectory;

  private transient ManagedThreadFactory threadFactory = new ManagedThreadFactory();

  private static FileCleaningTracker cleaner = new FileCleaningTracker();

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    TransferManager tm = wrapper.transferManager();
    File tempDir = null;
    if (!isEmpty(getTempDirectory())) {
      tempDir = new File(getTempDirectory());
    }
    GetObjectRequest request = new GetObjectRequest(s3Bucket(msg), s3ObjectKey(msg));
    log.debug("Getting {} from bucket {}", request.getKey(), request.getBucketName());
    File destFile = createTempFile(tempDir, msg);
    Download download = tm.download(request, destFile);
    threadFactory.newThread(new MyProgressListener(Thread.currentThread().getName(), download)).start();
    download.waitForCompletion();
    msg.setMetadata(filterUserMetadata(download.getObjectMetadata().getUserMetadata()));
    write(destFile, msg);
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
  
  public DownloadOperation withTempDirectory(String s) {
    setTempDirectory(s);
    return this;
  }

  private File createTempFile(File tempDir, Object marker) throws IOException {
    File f = File.createTempFile(this.getClass().getSimpleName(), "", tempDir);
    cleaner.track(f, Args.notNull(marker, "marker"), FileDeleteStrategy.FORCE);
    return f;
  }

  private class MyProgressListener implements Runnable {
    private Download download;
    private String name;

    MyProgressListener(String name, Download download) {
      this.download = download;
      this.name = name;
    }

    @Override
    public void run() {
      Thread.currentThread().setName(name);
      while (!download.isDone()) {
        log.trace("Downloaded : {}%", (download.getProgress().getPercentTransferred() / 1));
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }

}
