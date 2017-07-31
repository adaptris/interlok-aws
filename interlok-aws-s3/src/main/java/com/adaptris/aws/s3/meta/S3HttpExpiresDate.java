package com.adaptris.aws.s3.meta;

import java.util.Calendar;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.util.TimeInterval;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("s3-http-expires-date")
public class S3HttpExpiresDate extends S3ObjectMetadata {

  @NotNull
  @Valid
  private TimeInterval timeToLive;
  
  @Override
  public void apply(AdaptrisMessage msg, ObjectMetadata meta) throws ServiceException {
    if(getTimeToLive() == null) {
      throw new ServiceException("Time to Live must be specified");
    }
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MILLISECOND, (int)getTimeToLive().toMilliseconds());
    meta.setHttpExpiresDate(cal.getTime());
  }

  public TimeInterval getTimeToLive() {
    return timeToLive;
  }

  /**
   * Set how long after upload the object should remain cachable
   * @param timeToLive
   */
  public void setTimeToLive(TimeInterval timeToLive) {
    this.timeToLive = timeToLive;
  }

}
