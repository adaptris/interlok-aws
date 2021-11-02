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

package com.adaptris.aws2.s3;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.lms.FileBackedMessage;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ManagedThreadFactory;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FileCleaningTracker;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.IOUtils;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Download an object from S3.
 *
 * @config amazon-s3-download
 * @since 4.3.0
 */
@AdapterComponent
@ComponentProfile(summary = "Amazon S3 Download using Transfer Manager", since = "4.3.0")
@XStreamAlias("aws2-amazon-s3-download")
@DisplayOrder(order = {"bucket", "objectName", "tempDirectory", "userMetadataFilter"})
@NoArgsConstructor
public class DownloadOperation extends TransferOperation {

  @AdvancedConfig
  private String tempDirectory;

  private transient ManagedThreadFactory threadFactory = new ManagedThreadFactory();

  private static FileCleaningTracker cleaner = new FileCleaningTracker();

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    File tempDir = null;
    if (!isEmpty(getTempDirectory())) {
      tempDir = new File(getTempDirectory());
    }

    GetObjectRequest.Builder builder = GetObjectRequest.builder();
    builder.bucket(s3Bucket(msg));
    builder.key(s3ObjectKey(msg));
    GetObjectRequest request = builder.build();

    log.debug("Getting {} from bucket {}", request.key(), request.bucket());
    File destFile = createTempFile(tempDir, msg);

    S3Client s3Client = wrapper.amazonClient();
    ResponseInputStream<GetObjectResponse> responseStream = s3Client.getObject(request);

    try (FileOutputStream outputStream = new FileOutputStream(destFile))
    {
      IOUtils.copy(responseStream, outputStream);
    }

    msg.setMetadata(filterUserMetadata(responseStream.response().metadata()));
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
    tempDirectory = s;
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

}
