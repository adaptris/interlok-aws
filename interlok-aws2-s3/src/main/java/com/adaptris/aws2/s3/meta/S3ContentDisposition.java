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

@XStreamAlias("aws2-s3-content-disposition")
// @XStreamConverter(value = ToAttributedValueConverter.class, strings = { "contentDisposition" })
@NoArgsConstructor
public class S3ContentDisposition extends S3ObjectMetadata {

  /**
   * Sets the optional Content-Disposition HTTP header, which specifies presentational information
   * such as the recommended filename for the object to be saved as.
   *
   */
  @NotNull
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  @NonNull
  private String contentDisposition;

  @Override
  public void apply(AdaptrisMessage msg, Map<String, String> meta) throws ServiceException {
    Args.notNull(getContentDisposition(), "content-disposition");
    meta.put("Content-Disposition", msg.resolve(getContentDisposition()));
  }

}
