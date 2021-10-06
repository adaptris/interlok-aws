package com.adaptris.aws.s3.retry;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.aws.s3.AmazonS3Connection;
import com.adaptris.aws.s3.ClientWrapper;
import com.adaptris.aws.s3.RemoteBlobIterable;
import com.adaptris.aws.s3.UploadOperation;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.MetadataCollection;
import com.adaptris.core.http.jetty.retry.RetryStore;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.core.util.MessageHelper;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.cloud.RemoteBlob;
import com.adaptris.interlok.util.Args;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.S3Object;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @config amazon-s3-retry-store
 *
 */
@XStreamAlias("amazon-s3-retry-store")
@ComponentProfile(summary = "Supporting implementation for using S3 as your failed message store",
    tag = "retry,amazon,s3", since = "3.11.1", recommended = {AmazonS3Connection.class})
@NoArgsConstructor
@Slf4j
public class S3RetryStore implements RetryStore {

  protected static final String PAYLOAD_FILE_NAME = "payload.blob";
  protected static final String METADATA_FILE_NAME = "metadata.properties";
  protected static final String STACKTRACE_FILENAME = "stacktrace.txt";

  /**
   * Set the connection to use to connect to S3.
   *
   */
  @Valid
  @Getter
  @Setter
  @NotNull
  @NonNull
  private AdaptrisConnection connection;
  @Getter
  @Setter
  @NotBlank
  private String bucket;
  @Getter
  @Setter
  private String prefix;

  private transient Pattern nameMapper = null;

  @Override
  public void prepare() throws CoreException {
    Args.notBlank(bucket, "bucket");
    LifecycleHelper.prepare(getConnection());
  }

  @Override
  public void init() throws CoreException {
    nameMapper = Pattern.compile(payloadBlobRegexp());
    LifecycleHelper.init(getConnection());

  }

  @Override
  public void start() throws CoreException {
    LifecycleHelper.start(getConnection());

  }

  @Override
  public void stop() {
    LifecycleHelper.stop(getConnection());

  }

  @Override
  public void close() {
    LifecycleHelper.close(getConnection());
  }

  public S3RetryStore withConnection(AdaptrisConnection c) {
    setConnection(c);
    return this;
  }


  public S3RetryStore withPrefix(String s) {
    setPrefix(s);
    return this;
  }


  public S3RetryStore withBucket(String s) {
    setBucket(s);
    return this;
  }

  // Listing files is a little trickier since a ListObjectsRequest based on the configured
  // prefix gives us everything (not just the msg-id directories).
  // Filter on things that end with the required name 'payload.blob'
  // If the corresponding msg-id/metadata.properties doesn't exist, then it'll fail when we
  // attempt to retry it.
  @Override
  public Iterable<RemoteBlob> report() throws InterlokException {
    AmazonS3Client s3 = clientWrapper().amazonClient();
    ListObjectsV2Request request =
        new ListObjectsV2Request().withBucketName(getBucket()).withPrefix(getPrefix());
    return new RetryableBlobIterable(
        new RemoteBlobIterable(s3, request, (blob) -> blob.getName().endsWith(PAYLOAD_FILE_NAME)),
        (name) -> toMessageID(name));
  }

  @Override
  public boolean delete(String msgId) throws InterlokException {
    deleteObject(buildObjectName(msgId, PAYLOAD_FILE_NAME));
    deleteObject(buildObjectName(msgId, METADATA_FILE_NAME));
    deleteObject(buildObjectName(msgId, STACKTRACE_FILENAME));
    return true;
  }

  private void deleteObject(String objectName) throws InterlokException {
    AmazonS3Client s3 = clientWrapper().amazonClient();
    if (s3.doesObjectExist(getBucket(), objectName)) {
      s3.deleteObject(getBucket(), objectName);
      log.trace("Deleting {} from bucket {}", objectName, getBucket());
    }
  }


  @Override
  public void write(AdaptrisMessage msg) throws InterlokException {
    try {
      uploadPayload(msg);
      uploadMetadata(msg);
      uploadStacktrace(msg);
    } catch (Exception e) {
      throw ExceptionHelper.wrapInterlokException(e);
    }

  }

  private void uploadPayload(AdaptrisMessage msg) throws Exception {
    String blob = buildObjectName(msg.getUniqueId(), PAYLOAD_FILE_NAME);
    UploadOperation payloadUpload = uploader(blob);
    log.trace("Uploading {} to bucket {}", blob, getBucket());
    payloadUpload.execute(clientWrapper(), msg);
  }

  private void uploadMetadata(AdaptrisMessage msg) throws Exception {
    String metadata = buildObjectName(msg.getUniqueId(), METADATA_FILE_NAME);
    UploadOperation metadataUpload = uploader(metadata);
    AdaptrisMessage metadataMsg = metadataAsMessage(msg);
    log.trace("Uploading {} to bucket {}", metadata, getBucket());
    metadataUpload.execute(clientWrapper(), metadataMsg);
  }

  private void uploadStacktrace(AdaptrisMessage msg) throws Exception {
    String exception = buildObjectName(msg.getUniqueId(), STACKTRACE_FILENAME);
    Optional<String> stacktrace = MessageHelper.stackTraceAsString(msg);
    if (stacktrace.isPresent()) {
      UploadOperation stacktraceUpload = uploader(exception);
      AdaptrisMessage stacktraceMsg =
          AdaptrisMessageFactory.getDefaultInstance().newMessage(stacktrace.get());
      log.trace("Uploading {} to bucket {}", exception, getBucket());
      stacktraceUpload.execute(clientWrapper(), stacktraceMsg);
    }
  }

  @Override
  public AdaptrisMessage buildForRetry(String msgId, Map<String, String> metadata,
      AdaptrisMessageFactory factory) throws InterlokException {
    try {
      String payloadName = buildObjectName(msgId, PAYLOAD_FILE_NAME);
      AdaptrisMessage msg = AdaptrisMessageFactory.defaultIfNull(factory).newMessage();
      try (InputStream in = getInputStream(payloadName); OutputStream out = msg.getOutputStream()) {
        IOUtils.copy(in, out);
      }
      log.trace("Payload for [{}] loaded", msgId);
      msg.setMessageHeaders(metadata);
      msg.setUniqueId(msgId);
      return msg;
    } catch (Exception e) {
      throw ExceptionHelper.wrapInterlokException(e);
    }
  }

  @Override
  public Map<String, String> getMetadata(String msgId) throws InterlokException {
    try {
      String metadataName = buildObjectName(msgId, METADATA_FILE_NAME);
      Properties meta = new Properties();
      try (InputStream in = getInputStream(metadataName)) {
        meta.load(in);
      }
      log.trace("metadata for [{}] loaded", msgId);
      // The compiler works in mysterious ways.
      return (Map) meta;
    } catch (Exception e) {
      throw ExceptionHelper.wrapInterlokException(e);
    }
  }

  private InputStream getInputStream(String objectName) throws Exception {
    AmazonS3Client s3 = clientWrapper().amazonClient();
    GetObjectRequest request = new GetObjectRequest(getBucket(), objectName);
    log.trace("Getting {} from bucket {}", request.getKey(), request.getBucketName());
    S3Object response = s3.getObject(request);
    return response.getObjectContent();
  }

  private ClientWrapper clientWrapper() {
    return getConnection().retrieveConnection(ClientWrapper.class);
  }

  private UploadOperation uploader(String objectName) throws CoreException {
    UploadOperation op = new UploadOperation().withObjectName(objectName).withBucket(getBucket());
    // this is called for completeness, since we know we have an object name + bucket...
    // prepare() is only really required for the legacy migration stuffs.
    op.prepare();
    return op;
  }

  private AdaptrisMessage metadataAsMessage(AdaptrisMessage orig) throws Exception {
    AdaptrisMessage metadata = AdaptrisMessageFactory.getDefaultInstance().newMessage();
    try (OutputStream out = metadata.getOutputStream()) {
      Properties p = MetadataCollection.asProperties(orig.getMetadata());
      p.store(out, "");
    }
    return metadata;
  }

  // Side effect of the way the payloadBlobRegexp is built, regardless
  // the prefix-less match always matches... so we don't need to have
  // an if boundary on matches() we just need to call it.
  String toMessageID(String s) {
    Matcher m = nameMapper.matcher(s);
    m.matches();
    return m.toMatchResult().group(1);
  }

  String buildObjectName(String msgId, String name) {
    if (StringUtils.isBlank(getPrefix())) {
      return String.format("%s/%s", msgId, name);
    }
    return String.format("%s/%s/%s", getPrefix(), msgId, name);
  }

  String payloadBlobRegexp() {
    if (StringUtils.isBlank(getPrefix())) {
      return String.format("(.*)/%s", PAYLOAD_FILE_NAME);
    }
    return String.format("%s/(.*)/%s", getPrefix(), PAYLOAD_FILE_NAME);
  }
}
