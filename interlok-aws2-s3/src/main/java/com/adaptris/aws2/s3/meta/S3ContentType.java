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

package com.adaptris.aws2.s3.meta;

import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.interlok.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

import javax.validation.constraints.NotNull;
import java.util.Map;

@XStreamAlias("s3-content-type")
// @XStreamConverter(value = ToAttributedValueConverter.class, strings = { "contentType" })
@NoArgsConstructor
public class S3ContentType extends S3ObjectMetadata {

  /**
   * Sets the Content-Type HTTP header indicating the type of content stored in the associated
   * object.
   * <p>
   * The value of this header is a standard MIME type. When uploading files, the AWS S3 Java client
   * will attempt to determine the correct content type if one hasn't been set yet. Users are
   * responsible for ensuring a suitable content type is set when uploading streams. If no content
   * type is provided and cannot be determined by the filename, the default content type
   * "application/octet-stream" will be used.
   * </p>
   */
  @NotNull
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  @NonNull
  private String contentType;

  @Override
  public void apply(AdaptrisMessage msg, Map<String, String> meta) throws ServiceException {
    Args.notNull(getContentType(), "content-type");
    meta.put("Content-Type", msg.resolve(getContentType()));
  }

}
