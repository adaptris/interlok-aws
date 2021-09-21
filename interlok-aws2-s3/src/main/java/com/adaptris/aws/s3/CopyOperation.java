/*
 * Copyright 2018 Adaptris
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.adaptris.aws.s3;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ObjectUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;

/**
 * Copy an object from S3 to another object
 *
 * <p>
 * Effectively uses {@link S3Client#copyObject(CopyObjectRequest)} using only the
 * default behaviour. Note that by default the {@code server-side-encryption, storage-class} and
 * {@code website-redirect-location} are not copied to the destination object. If you need those
 * settings then you should probably think about using {@link ExtendedCopyOperation} instead.
 * </p>
 *
 * @config amazon-s3-copy
 */
@AdapterComponent
@ComponentProfile(summary = "Copy an object in S3 to another Object")
@XStreamAlias("amazon-s3-copy")
@DisplayOrder(order = {"bucket", "objectName", "destinationBucket", "destinationObjectName"})
@NoArgsConstructor
public class CopyOperation extends CopyOperationImpl {

  @Override
  public void prepare() throws CoreException {
    super.prepare();
    Args.notBlank(getDestinationObjectName(), "destination-object-name");
  }

  @Override
  protected CopyObjectRequest createCopyRequest(ClientWrapper wrapper, AdaptrisMessage msg)
      throws Exception {
    CopyObjectRequest.Builder builder = CopyObjectRequest.builder();
    builder.sourceBucket(s3Bucket(msg));
    builder.sourceKey(s3ObjectKey(msg));
    builder.destinationBucket(destinationBucket(msg));
    builder.destinationKey(destinationKey(msg));
    return builder.build();
  }


  private String destinationKey(AdaptrisMessage msg) {
    return msg.resolve(getDestinationObjectName(), true);
  }

  private String destinationBucket(AdaptrisMessage msg) throws InterlokException {
    return ObjectUtils.defaultIfNull(msg.resolve(getDestinationBucket(), true), s3Bucket(msg));
  }
}
