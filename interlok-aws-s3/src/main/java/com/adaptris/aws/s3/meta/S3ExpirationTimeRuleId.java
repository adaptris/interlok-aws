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

package com.adaptris.aws.s3.meta;

import javax.validation.constraints.NotNull;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.interlok.util.Args;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("s3-expiration-rule-id")
// @XStreamConverter(value = ToAttributedValueConverter.class, strings = { "expirationRuleId" })
public class S3ExpirationTimeRuleId extends S3ObjectMetadata {

  @NotNull
  @InputFieldHint(expression = true)
  private String expirationRuleId;
  
  @Override
  public void apply(AdaptrisMessage msg, ObjectMetadata meta) throws ServiceException {
    Args.notNull(getExpirationTimeRuleId(), "expiration-rule-id");
    meta.setExpirationTimeRuleId(msg.resolve(getExpirationTimeRuleId()));
  }

  public String getExpirationTimeRuleId() {
    return expirationRuleId;
  }

  /**
   * Sets the BucketLifecycleConfiguration rule ID for this object's expiration.
   */
  public void setExpirationTimeRuleId(String expirationRuleId) {
    this.expirationRuleId = expirationRuleId;
  }

}
