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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.http.util.Args;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.interlok.config.DataInputParameter;

/**
 * Abstract base class for S3 Operations.
 * 
 * @author lchan
 *
 */
public abstract class S3OperationImpl implements S3Operation {
  protected transient Logger log = LoggerFactory.getLogger(this.getClass().getName());


  @NotNull
  @Valid
  private DataInputParameter<String> bucketName;
  @NotNull
  @Valid
  private DataInputParameter<String> key;

  public S3OperationImpl() {
  }

  public DataInputParameter<String> getKey() {
    return key;
  }

  public void setKey(DataInputParameter<String> key) {
    this.key = Args.notNull(key, "key");
  }

  public <T extends S3OperationImpl> T withKey(DataInputParameter<String> key) {
    setKey(key);
    return (T) this;
  }
  
  public DataInputParameter<String> getBucketName() {
    return bucketName;
  }

  public void setBucketName(DataInputParameter<String> bucketName) {
    this.bucketName = Args.notNull(bucketName, "bucketName");
  }

  public <T extends S3OperationImpl> T withBucketName(DataInputParameter<String> key) {
    setBucketName(key);
    return (T) this;
  }
  
}
