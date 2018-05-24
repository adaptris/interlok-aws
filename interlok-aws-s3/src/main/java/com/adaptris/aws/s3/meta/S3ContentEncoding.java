package com.adaptris.aws.s3.meta;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.apache.commons.lang.StringUtils;

import javax.validation.constraints.NotNull;

@XStreamAlias("s3-content-encoding")
// @XStreamConverter(value = ToAttributedValueConverter.class, strings = { "contentEncoding" })
public class S3ContentEncoding extends S3ObjectMetadata {

  @NotNull
  @InputFieldHint(expression = true)
  private String contentEncoding;

  @Override
  public void apply(AdaptrisMessage msg, ObjectMetadata meta) throws ServiceException {
    if(StringUtils.isEmpty(getContentEncoding())) {
      throw new ServiceException("Content Encoding must be specified");
    }
    meta.setContentEncoding(msg.resolve(getContentEncoding()));
  }

  public String getContentEncoding() {
    return contentEncoding;
  }

  /**
   * Sets the optional Content-Encoding HTTP header specifying what
   * content encodings have been applied to the object and what decoding
   * mechanisms must be applied in order to obtain the media-type referenced
   * by the Content-Type field.
   *
   * @param contentEncoding
   */
  public void setContentEncoding(String contentEncoding) {
    this.contentEncoding = contentEncoding;
  }

}
