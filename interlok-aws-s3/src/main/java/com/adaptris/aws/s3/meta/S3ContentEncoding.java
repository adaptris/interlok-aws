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

@XStreamAlias("s3-content-encoding")
// @XStreamConverter(value = ToAttributedValueConverter.class, strings = { "contentEncoding" })
public class S3ContentEncoding extends S3ObjectMetadata {

  @NotNull
  @InputFieldHint(expression = true)
  private String contentEncoding;

  @Override
  public void apply(AdaptrisMessage msg, ObjectMetadata meta) throws ServiceException {
    Args.notNull(getContentEncoding(), "content-encoding");
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
