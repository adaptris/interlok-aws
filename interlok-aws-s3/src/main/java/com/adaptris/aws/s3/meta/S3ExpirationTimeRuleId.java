package com.adaptris.aws.s3.meta;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamConverter;
import com.thoughtworks.xstream.converters.extended.ToAttributedValueConverter;

@XStreamAlias("s3-expiration-rule-id")
@XStreamConverter(value = ToAttributedValueConverter.class, strings = { "expirationRuleId" })
public class S3ExpirationTimeRuleId extends S3ObjectMetadata {

  @NotNull
  @InputFieldHint(expression = true)
  private String expirationRuleId;
  
  @Override
  public void apply(AdaptrisMessage msg, ObjectMetadata meta) throws ServiceException {
    if(StringUtils.isEmpty(getExpirationTimeRuleId())) {
      throw new ServiceException("Expiration Time Rule Id must be specified");
    }
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
