package com.adaptris.aws.s3.meta;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("s3-content-type")
// @XStreamConverter(value = ToAttributedValueConverter.class, strings = { "contentType" })
public class S3ContentType extends S3ObjectMetadata {

  @NotNull
  @InputFieldHint(expression = true)
  private String contentType;

  @Override
  public void apply(AdaptrisMessage msg, ObjectMetadata meta) throws ServiceException {
    if(StringUtils.isEmpty(getContentType())) {
      throw new ServiceException("Content Type must be specified");
    }
    meta.setContentType(msg.resolve(getContentType()));
  }

  public String getContentType() {
    return contentType;
  }

  /**
   * Sets the Content-Type HTTP header indicating the type of content stored in the associated object. 
   * The value of this header is a standard MIME type.
   * <br/>
   * When uploading files, the AWS S3 Java client will attempt to determine the correct content type if 
   * one hasn't been set yet. Users are responsible for ensuring a suitable content type is set when 
   * uploading streams. If no content type is provided and cannot be determined by the filename, the default 
   * content type "application/octet-stream" will be used.
   * 
   * @param contentType
   */
  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

}
