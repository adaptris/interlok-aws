package com.adaptris.aws2.s3;

import com.adaptris.interlok.cloud.RemoteBlob;
import com.adaptris.interlok.cloud.RemoteBlobFilter;
import com.adaptris.interlok.cloud.RemoteBlobIterableImpl;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

public class RemoteBlobIterable extends RemoteBlobIterableImpl<S3Object> {

  private S3Client s3Client = null;
  private ListObjectsV2Request listRequest = null;
  private RemoteBlobFilter blobFilter = null;

  private ListObjectsV2Response currentListing;
  private Iterator<S3Object> currentListingIterator;


  public RemoteBlobIterable(S3Client s3, ListObjectsV2Request request, RemoteBlobFilter filter) {
    s3Client = s3;
    listRequest = request;
    blobFilter = filter;
  }

  @Override
  protected Optional<S3Object> nextStorageItem() throws NoSuchElementException {
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
    String token = currentListing.nextContinuationToken();
    ListObjectsV2Request.Builder builder = listRequest.toBuilder();
    builder.continuationToken(token);
    listRequest = builder.build();
    currentListing = s3Client.listObjectsV2(listRequest);
    currentListingIterator = currentListing.contents().iterator();
  }

  @Override
  protected Optional<RemoteBlob> accept(S3Object summary) {
    String bucket = listRequest.bucket();
    RemoteBlob blob = new RemoteBlob.Builder().setBucket(bucket).setLastModified(summary.lastModified().getEpochSecond())
        .setName(summary.key()).setSize(summary.size()).build();
    if (blobFilter.accept(blob)) {
      return Optional.of(blob);
    }
    return Optional.empty();
  }

  @Override
  protected void iteratorInit() {
    currentListing = s3Client.listObjectsV2(listRequest);
    currentListingIterator = currentListing.contents().iterator();
  }
}
