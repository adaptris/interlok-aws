package com.adaptris.aws.s3.meta;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamConverter;
import com.thoughtworks.xstream.converters.extended.ToAttributedValueConverter;

@XStreamAlias("s3-content-disposition")
@XStreamConverter(value = ToAttributedValueConverter.class, strings = { "contentDisposition" })
public class S3ContentDisposition extends S3ObjectMetadata {

  @NotNull
  @InputFieldHint(expression = true)
  private String contentDisposition;

  @Override
  public void apply(AdaptrisMessage msg, ObjectMetadata meta) throws ServiceException {
    if(StringUtils.isEmpty(getContentDisposition())) {
      throw new ServiceException("Content Disposition must be specified");
    }
    meta.setContentDisposition(msg.resolve(getContentDisposition()));
  }

  public String getContentDisposition() {
    return contentDisposition;
  }

  /**
   * Sets the optional Content-Disposition HTTP header, which specifies 
   * presentational information such as the recommended filename for the object to be saved as.
   * 
   * @param contentDisposition
   */
  public void setContentDisposition(String contentDisposition) {
    this.contentDisposition = contentDisposition;
  }

}
