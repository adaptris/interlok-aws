package com.adaptris.aws.s3;

import java.util.ArrayList;
import java.util.List;

import com.adaptris.aws.DefaultAWSAuthentication;
import com.adaptris.aws.s3.meta.S3ContentLanguage;
import com.adaptris.aws.s3.meta.S3ServerSideEncryption;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.common.PayloadStreamOutputParameter;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.adaptris.core.metadata.RemoveAllMetadataFilter;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

public class S3ServiceTest extends ServiceCase {
  private static final String HYPHEN = "-";

  private enum OperationsBuilder {

    Download {
      @Override
      S3Operation build() {
        DownloadOperation op = new DownloadOperation();
        op.setKey(new ConstantDataInputParameter("s3-key"));
        op.setBucketName(new ConstantDataInputParameter("s3-bucket"));
        op.setTempDirectory("/path/to/temp/dir/if/required");
        return op;
      }
      
    },
    Get {
      @Override
      S3Operation build() {
        S3GetOperation op = new S3GetOperation();
        op.setKey(new ConstantDataInputParameter("s3-key"));
        op.setBucketName(new ConstantDataInputParameter("s3-bucket"));
        op.setResponseBody(new PayloadStreamOutputParameter());
        return op;
      }
    },
    Upload {
      @Override
      S3Operation build() {
        UploadOperation op = new UploadOperation();
        op.setKey(new ConstantDataInputParameter("s3-key"));
        op.setBucketName(new ConstantDataInputParameter("s3-bucket"));
        op.setUserMetadataFilter(new RemoveAllMetadataFilter());
        S3ContentLanguage type = new S3ContentLanguage();
        type.setContentLanguage("english");
        op.getObjectMetadata().add(new S3ServerSideEncryption());
        op.getObjectMetadata().add(type);
        return op;
      }
    },
    Tag {
      @Override
      S3Operation build() {
        TagOperation op = new TagOperation();
        op.setKey(new ConstantDataInputParameter("s3-key"));
        op.setBucketName(new ConstantDataInputParameter("s3-bucket"));
        op.setTagMetadataFilter(new NoOpMetadataFilter());
        return op;
      }
    };
    abstract S3Operation build();
  }

  public S3ServiceTest(String name) {
    super(name);
  }

  @Override
  protected S3Service retrieveObjectForSampleConfig() {
    return null;
  }

  @Override
  protected final List retrieveObjectsForSampleConfig() {
    ArrayList result = new ArrayList();
    for (OperationsBuilder b : OperationsBuilder.values()) {
      result.add(new S3Service(new AmazonS3Connection(new DefaultAWSAuthentication(), exampleClientConfig()), b.build()));
    }
    return result;
  }

  protected KeyValuePairSet exampleClientConfig() {
    KeyValuePairSet kvps = new KeyValuePairSet();
    kvps.add(new KeyValuePair("ProxyHost", "my.proxy.host"));
    kvps.add(new KeyValuePair("ProxyPort", "3128"));
    kvps.add(new KeyValuePair("ConnectionTimeout", "60000"));
    kvps.add(new KeyValuePair("Reaper", "true"));
    return kvps;
  }

  @Override
  protected String createBaseFileName(Object object) {
    return super.createBaseFileName(object) + HYPHEN + ((S3Service) object).getOperation().getClass().getSimpleName();
  }

}
