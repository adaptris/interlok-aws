package com.adaptris.aws.kinesis;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

public interface RequestBuilder {

  Iterable<PutRecordsRequestEntry> build(String partitionKey, AdaptrisMessage message) throws CoreException;
}
