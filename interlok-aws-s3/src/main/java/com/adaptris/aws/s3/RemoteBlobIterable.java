package com.adaptris.aws.s3;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import com.adaptris.interlok.cloud.RemoteBlob;
import com.adaptris.interlok.cloud.RemoteBlobFilter;
import com.adaptris.interlok.cloud.RemoteBlobIterableImpl;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class RemoteBlobIterable extends RemoteBlobIterableImpl<S3ObjectSummary> {

  private AmazonS3Client s3Client = null;
  private ListObjectsV2Request listRequest = null;
  private RemoteBlobFilter blobFilter = null;

  private ListObjectsV2Result currentListing;
  private Iterator<S3ObjectSummary> currentListingIterator;


  public RemoteBlobIterable(AmazonS3Client s3, ListObjectsV2Request request,
      RemoteBlobFilter filter) {
    s3Client = s3;
    listRequest = request;
    blobFilter = filter;
  }

  @Override
  protected Optional<S3ObjectSummary> nextStorageItem() throws NoSuchElementException {
    if (!currentListingIterator.hasNext()) {
      advanceToNextPage();
    }
    return Optional.ofNullable(currentListingIterator.next());
  }

  private void advanceToNextPage() throws NoSuchElementException {
    if (!currentListing.isTruncated()) {
      // it's not truncated so there's nothing else to get
      throw new NoSuchElementException();
    }
    String token = currentListing.getNextContinuationToken();
    listRequest.setContinuationToken(token);
    currentListing = s3Client.listObjectsV2(listRequest);
    currentListingIterator = currentListing.getObjectSummaries().iterator();
  }

  @Override
  protected Optional<RemoteBlob> accept(S3ObjectSummary summary) {
    String bucket = StringUtils.defaultIfEmpty(summary.getBucketName(), listRequest.getBucketName());
    RemoteBlob blob = new RemoteBlob.Builder().setBucket(bucket).setLastModified(summary.getLastModified().getTime())
        .setName(summary.getKey()).setSize(summary.getSize()).build();
    if (blobFilter.accept(blob)) {
      return Optional.of(blob);
    }
    return Optional.empty();
  }

  @Override
  protected void iteratorInit() {
    currentListing = s3Client.listObjectsV2(listRequest);
    currentListingIterator = currentListing.getObjectSummaries().iterator();
  }
}
