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

import javax.validation.Valid;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
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

  private transient boolean bucketNameWarningLogged = false;
  private transient boolean keyNameWarningLogged = false;

  @AdvancedConfig(rare = true)
  @Getter
  @Setter
  @Valid
  @Deprecated
  @Removal(version = "3.12.0", message = "Use an expression based bucket instead")
  private DataInputParameter<String> bucketName;
  @AdvancedConfig(rare = true)
  @Getter
  @Setter
  @Valid
  @Deprecated
  @Removal(version = "3.12.0", message = "Use an expression based blob-name/prefix instead")
  private DataInputParameter<String> key;

  @Getter
  @Setter
  @InputFieldHint(expression = true)
  private String bucket;


  public <T extends S3OperationImpl> T withKey(DataInputParameter<String> key) {
    setKey(key);
    return (T) this;
  }

  public <T extends S3OperationImpl> T withBucketName(DataInputParameter<String> key) {
    setBucketName(key);
    return (T) this;
  }

  public <T extends S3OperationImpl> T withBucket(String b) {
    setBucket(b);
    return (T) this;
  }

  @Override
  public void prepare() throws CoreException {
    if (getBucketName() != null) {
      LoggingHelper.logWarning(bucketNameWarningLogged, () -> bucketNameWarningLogged = true,
          "[{}] uses [bucket-name], use [bucket] instead", this.getClass().getSimpleName());
    }
    if (getKey() != null) {
      LoggingHelper.logWarning(keyNameWarningLogged, () -> keyNameWarningLogged = true,
          "[{}] uses [key], use the alternative string-based expression instead",
          this.getClass().getSimpleName());
    }
    mustHaveEither(getBucketName(), getBucket());
  }

  protected static void mustHaveEither(DataInputParameter<String> legacy, String expression) {
    if (BooleanUtils.and(new boolean[] {legacy == null, StringUtils.isBlank(expression)})) {
      throw new IllegalArgumentException("both data-input param and expression are empty");
    }
  }

  protected String s3Bucket(AdaptrisMessage msg) throws InterlokException {
    return resolve(getBucketName(), getBucket(), msg);
  }

  protected static String resolve(DataInputParameter<String> legacy, String expression,
      AdaptrisMessage msg) throws InterlokException {
    String result = msg.resolve(expression, true);
    if (legacy != null) {
      result = legacy.extract(msg);
    }
    return result;
  }
}
