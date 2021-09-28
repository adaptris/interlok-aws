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

@XStreamAlias("s3-content-language")
// @XStreamConverter(value = ToAttributedValueConverter.class, strings = { "contentLanguage" })
@NoArgsConstructor
public class S3ContentLanguage extends S3ObjectMetadata {

  /**
   * Sets the Content-Language HTTP header which describes the natural language(s) of the intended
   * audience for the enclosed entity.
   *
   * @param contentLanguage
   */
  @NotNull
  @Getter
  @Setter
  @NonNull
  @InputFieldHint(expression = true)
  private String contentLanguage;

  @Override
  public void apply(AdaptrisMessage msg, Map<String, String> meta) throws ServiceException {
    Args.notNull(getContentLanguage(), "content-language");
    meta.put("Content-Language", msg.resolve(getContentLanguage()));
  }
}
