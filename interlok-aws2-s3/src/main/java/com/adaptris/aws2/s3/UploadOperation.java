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
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.aws2.s3.acl.S3ObjectAcl;
import com.adaptris.aws2.s3.meta.S3ObjectMetadata;
import com.adaptris.core.AdaptrisMessage;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.ObjectUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.validation.Valid;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Upload an object to S3.
 *
 * @config amazon-s3-upload
 * @since 4.3.0
 */
@AdapterComponent
@ComponentProfile(summary = "Amazon S3 Upload using Transfer Manager", since = "4.3.0")
@XStreamAlias("aws2-amazon-s3-upload")
@DisplayOrder(
    order = {"bucket", "objectName", "userMetadataFilter", "cannedObjectAcl", "objectAcl", "objectMetadata"})
public class UploadOperation extends TransferOperation {

  @Getter
  @Setter
  @AdvancedConfig
  @Valid
  private List<S3ObjectMetadata> objectMetadata;

  @Getter
  @Setter
  @AdvancedConfig
  @InputFieldHint(expression = true, style = "com.adaptris.aws2.s3.S3ObjectCannedAcl")
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

    S3Client s3Client = wrapper.amazonClient();

    String bucketName = s3Bucket(msg);
    String key = s3ObjectKey(msg);

    try (InputStream in = msg.getInputStream()) {
      log.debug("Uploading to {} in bucket {}", key, bucketName);

      PutObjectRequest.Builder builder = PutObjectRequest.builder();

      builder.bucket(bucketName);
      builder.key(key);
      builder.contentLength(msg.getSize());
      builder.contentEncoding(msg.getContentEncoding());
      builder.metadata(filterMetadata(msg));

      if(!isEmpty(getCannedObjectAcl())) {
        builder.acl(S3ObjectCannedAcl.valueOf(msg.resolve(getCannedObjectAcl())).getCannedAccessControl());
      }
//      if(getObjectAcl() != null) {
//        builder.acl(getObjectAcl().create());
//      }

      RequestBody requestBody = RequestBody.fromInputStream(in, msg.getSize());

      s3Client.putObject(builder.build(), requestBody);
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
}
