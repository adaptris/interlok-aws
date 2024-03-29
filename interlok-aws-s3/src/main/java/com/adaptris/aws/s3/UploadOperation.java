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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.validation.Valid;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.aws.s3.acl.S3ObjectAcl;
import com.adaptris.aws.s3.meta.S3ObjectMetadata;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.util.ManagedThreadFactory;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import lombok.Getter;
import lombok.Setter;

/**
 * Upload an object to S3 using {@link TransferManager}.
 *
 * @author lchan
 * @config amazon-s3-upload
 */
@AdapterComponent
@ComponentProfile(summary = "Amazon S3 Upload using Transfer Manager")
@XStreamAlias("amazon-s3-upload")
@DisplayOrder(
    order = {"bucket", "objectName", "userMetadataFilter", "cannedObjectAcl", "objectAcl", "objectMetadata"})
public class UploadOperation extends TransferOperation {

  private transient ManagedThreadFactory threadFactory = new ManagedThreadFactory();

  @Getter
  @Setter
  @AdvancedConfig
  @Valid
  private List<S3ObjectMetadata> objectMetadata;

  @Getter
  @Setter
  @AdvancedConfig
  @InputFieldHint(expression = true, style = "com.adaptris.aws.s3.S3ObjectCannedAcl")
  private String cannedObjectAcl;

  /**
   * Sets the optional access control list for the new object. If specified,
   * cannedObjectAcl will be ignored.
   */
  @Getter
  @Setter
  @AdvancedConfig
  @Valid
  private S3ObjectAcl objectAcl;

  @Override
  public void execute(ClientWrapper wrapper, AdaptrisMessage msg) throws Exception {
    TransferManager tm = wrapper.transferManager();
    String bucketName = s3Bucket(msg);
    String key = s3ObjectKey(msg);
    ObjectMetadata s3meta = new ObjectMetadata();
    s3meta.setContentLength(msg.getSize());
    if(StringUtils.isNotEmpty(msg.getContentEncoding())) {
      s3meta.setContentEncoding(msg.getContentEncoding());
    }
    s3meta.setUserMetadata(filterMetadata(msg));
    for(S3ObjectMetadata m: objectMetadata()) {
      m.apply(msg, s3meta);
    }
    try (InputStream in = msg.getInputStream()) {
      log.debug("Uploading to {} in bucket {}", key, bucketName);
      PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, in, s3meta);
      if(!isEmpty(getCannedObjectAcl())) {
        putObjectRequest.setCannedAcl(S3ObjectCannedAcl.valueOf(msg.resolve(getCannedObjectAcl())).getCannedAccessControl());
      }
      if(getObjectAcl() != null) {
        putObjectRequest.setAccessControlList(getObjectAcl().create());
      }
      Upload upload = tm.upload(putObjectRequest);
      threadFactory.newThread(new MyProgressListener(Thread.currentThread().getName(), upload)).start();
      upload.waitForCompletion();
    }
  }

  public UploadOperation withObjectMetadata(List<S3ObjectMetadata> objectMetadata) {
    setObjectMetadata(objectMetadata);
    return this;
  }

  public UploadOperation withObjectMetadata(S3ObjectMetadata... objectMetadata) {
    return withObjectMetadata(new ArrayList<>(Arrays.asList(objectMetadata)));
  }

  private List<S3ObjectMetadata> objectMetadata() {
    return ObjectUtils.defaultIfNull(getObjectMetadata(), Collections.emptyList());
  }

  public UploadOperation withCannedObjectAcl(String objectAcl) {
    setCannedObjectAcl(objectAcl);
    return this;
  }

  public UploadOperation withObjectAcl(S3ObjectAcl objectAcl) {
    setObjectAcl(objectAcl);
    return this;
  }

  private class MyProgressListener implements Runnable {
    private Upload upload;
    private String name;

    MyProgressListener(String name, Upload upload) {
      this.upload = upload;
      this.name = name;
    }

    @Override
    public void run() {
      Thread.currentThread().setName(name);
      while (!upload.isDone()) {
        log.trace("Uploaded : {}%", upload.getProgress().getPercentTransferred() / 1);
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }
}
