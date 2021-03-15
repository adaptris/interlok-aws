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

import javax.validation.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.util.Args;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Abstract base class for S3 Operations.
 *
 *
 */
@NoArgsConstructor
public abstract class S3OperationImpl implements S3Operation {
  protected transient Logger log = LoggerFactory.getLogger(this.getClass().getName());

  @Getter
  @Setter
  @InputFieldHint(expression = true)
  @NotBlank
  private String bucket;


  public <T extends S3OperationImpl> T withBucket(String b) {
    setBucket(b);
    return (T) this;
  }

  @Override
  public void prepare() throws CoreException {
    Args.notBlank(getBucket(), "bucket");
  }

  protected String s3Bucket(AdaptrisMessage msg) throws InterlokException {
    return resolve(getBucket(), msg);
  }

  /**
   * @deprecated just use msg.resolve() instead.
   */
  @Deprecated
  protected static String resolve(String expression, AdaptrisMessage msg) throws InterlokException {
    return msg.resolve(expression, true);
  }
}
