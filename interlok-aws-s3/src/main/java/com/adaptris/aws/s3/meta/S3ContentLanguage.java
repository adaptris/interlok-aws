package com.adaptris.aws.s3.meta;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("s3-content-language")
// @XStreamConverter(value = ToAttributedValueConverter.class, strings = { "contentLanguage" })
public class S3ContentLanguage extends S3ObjectMetadata {

  @NotNull
  @InputFieldHint(expression = true)
  private String contentLanguage;
  
  @Override
  public void apply(AdaptrisMessage msg, ObjectMetadata meta) throws ServiceException {
    if(StringUtils.isEmpty(getContentLanguage())) {
      throw new ServiceException("Content Language must be specified");
    }
    meta.setContentLanguage(msg.resolve(getContentLanguage()));
  }

  public String getContentLanguage() {
    return contentLanguage;
  }

  /**
   * Sets the Content-Language HTTP header which describes the natural 
   * language(s) of the intended audience for the enclosed entity.
   * 
   * @param contentLanguage
   */
  public void setContentLanguage(String contentLanguage) {
    this.contentLanguage = contentLanguage;
  }

}
