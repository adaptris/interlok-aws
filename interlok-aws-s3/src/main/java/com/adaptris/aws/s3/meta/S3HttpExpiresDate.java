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

import java.util.Calendar;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.interlok.util.Args;
import com.adaptris.util.TimeInterval;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

@XStreamAlias("s3-http-expires-date")
@NoArgsConstructor
public class S3HttpExpiresDate extends S3ObjectMetadata {

  /**
   * Set how long after upload the object should remain cachable
   *
   */
  @NotNull
  @Valid
  @Getter
  @Setter
  @NonNull
  private TimeInterval timeToLive;

  @Override
  public void apply(AdaptrisMessage msg, ObjectMetadata meta) throws ServiceException {
    Args.notNull(getTimeToLive(), "time-to-live");
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MILLISECOND, (int)getTimeToLive().toMilliseconds());
    meta.setHttpExpiresDate(cal.getTime());
  }

}
