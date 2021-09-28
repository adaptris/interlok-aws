package com.adaptris.aws2.s3;

import javax.validation.constraints.NotBlank;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.core.AdaptrisConnection;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ConnectedService;
import com.adaptris.core.CoreException;
import com.adaptris.core.DynamicPollingTemplate;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.cloud.BlobListRenderer;
import com.adaptris.interlok.cloud.RemoteBlobFilter;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

/**
 * Query a S3 Location for a list of blobs stored there.
 * <p>
 * Under the covers it re-uses {@link S3Service} with a {@link ListOperation} and does a full
 * lifecycle on the underlying service each time it is triggered. This is intended for use as part
 * of a {@link DynamicPollingTemplate}; as a result keys are not intended to be resolved using the
 * {@code %message} expression language; they will, however, be passed as-is into the underlying
 * service (which may still resolve them).
 * </p>
 *
 * @config s3-bucket-list
 */
@XStreamAlias("s3-bucket-list")
@ComponentProfile(summary = "List contents of an S3 bucket as part of a polling-trigger", since = "3.9.2", tag = "aws2,s3,polling")
@DisplayOrder(order = {"connection", "bucket", "prefix", "key", "filter"})
public class S3BucketList extends ServiceImp implements DynamicPollingTemplate.TemplateProvider, ConnectedService {

  @Setter
  @Getter
  private AdaptrisConnection connection;

  @Setter
  @Getter
  private BlobListRenderer outputStyle;

  /**
   * The S3 bucket to connect to.
   *
   */
  @NotBlank
  @Setter
  @Getter
  @NonNull
  private String bucket;

  /**
   * The prefix to use when issuing the listOperation
   *
   */
  @Setter
  @Getter
  private String prefix;

  /**
   * Specify any additional filtering you wish to perform on the list.
   *
   */
  @Getter
  @Setter
  @AdvancedConfig
  private RemoteBlobFilter filter;

  /**
   * Specify max number of keys to be returned.
   */
  @AdvancedConfig(rare=true)
  @Getter
  @Setter
  private Integer maxKeys;

  @Override
  public void prepare() throws CoreException {
  }

  @Override
  protected void initService() throws CoreException {}

  @Override
  protected void closeService() {}

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    S3Service service = buildService();
    try {
      LifecycleHelper.initAndStart(service, false);
      service.doService(msg);
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    } finally {
      LifecycleHelper.stopAndClose(service, false);
    }
  }

  public S3BucketList withConnection(AdaptrisConnection c) {
    setConnection(c);
    return this;
  }


  public S3BucketList withPrefix(String key) {
    setPrefix(key);
    return this;
  }

  public S3BucketList withBucket(String bucket) {
    setBucket(bucket);
    return this;
  }

  public S3BucketList withFilter(RemoteBlobFilter f) {
    setFilter(f);
    return this;
  }


  public S3BucketList withMaxKeys(Integer maxKeys){
    setMaxKeys(maxKeys);
    return this;
  }

  public S3BucketList withOutputStyle(BlobListRenderer outputStyle) {
    setOutputStyle(outputStyle);
    return this;
  }

  private S3Service buildService() {
    ListOperation op = new ListOperation().withFilter(getFilter()).withOutputStyle(getOutputStyle())
        .withMaxKeys(getMaxKeys())
        .withPrefix(getPrefix())
        .withBucket(getBucket());
    return new S3Service(getConnection(), op);
  }
}
