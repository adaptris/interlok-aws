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
import com.adaptris.validation.constraints.ConfigDeprecated;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.core.util.LoggingHelper;
import com.adaptris.interlok.util.Args;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

@XStreamAlias("s3-expiration-rule-id")
// @XStreamConverter(value = ToAttributedValueConverter.class, strings = { "expirationRuleId" })
@NoArgsConstructor
public class S3ExpirationTimeRuleId extends S3ObjectMetadata {

  private transient boolean warningLogged = false;

  /**
   * Sets the BucketLifecycleConfiguration rule ID for this object's expiration.
   *
   * @deprecated since 3.10.2 naming mismatch, use {@link #setExpirationTimeRuleId(String)} instead.
   *
   */
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  @Deprecated
  @ConfigDeprecated(removalVersion = "3.12.0", message = "naming mismatch, use 'expiration-time-rule-id' instead", groups = Deprecated.class)
  private String expirationRuleId;

  /**
   * Sets the BucketLifecycleConfiguration rule ID for this object's expiration.
   */
  @NotNull
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  @NonNull
  private String expirationTimeRuleId;

  @Override
  public void apply(AdaptrisMessage msg, ObjectMetadata meta) throws ServiceException {
    String ruleId = Args.notNull(ruleIdSelector(), "expiration rule");
    meta.setExpirationTimeRuleId(msg.resolve(ruleId));
  }

  private String ruleIdSelector() {
    if (getExpirationRuleId() != null) {
      LoggingHelper.logDeprecation(warningLogged, () -> warningLogged = true, "expiration-rule-id",
          "expiration-time-rule-id");
      return getExpirationRuleId();
    }
    return getExpirationTimeRuleId();
  }

}
