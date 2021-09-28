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

package com.adaptris.aws2.s3;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.mockito.Mockito;
import com.adaptris.aws2.DefaultAWSAuthentication;
import com.adaptris.aws2.StaticCredentialsBuilder;
import com.adaptris.aws2.s3.meta.S3ContentLanguage;
import com.adaptris.aws2.s3.meta.S3ServerSideEncryption;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.common.PayloadStreamOutputParameter;
import com.adaptris.core.metadata.NoOpMetadataFilter;
import com.adaptris.core.metadata.RemoveAllMetadataFilter;
import com.adaptris.core.util.LifecycleHelper;
import com.adaptris.interlok.junit.scaffolding.services.ExampleServiceCase;
import com.adaptris.util.KeyValuePair;
import com.adaptris.util.KeyValuePairSet;

public class S3ServiceTest extends ExampleServiceCase {
  private static final String HYPHEN = "-";

  private enum OperationsBuilder {

    Download {
      @Override
      S3Operation build() {
        DownloadOperation op = new DownloadOperation();
        op.setObjectName("s3-key");
        op.setBucket("%message{s3-bucket-key}");
        op.setTempDirectory("/path/to/temp/dir/if/required");
        return op;
      }

    },
    Get {
      @Override
      S3Operation build() {
        S3GetOperation op = new S3GetOperation();
        op.setObjectName("s3-key");
        op.setBucket("%message{s3-bucket-key}");
        op.setResponseBody(new PayloadStreamOutputParameter());
        return op;
      }
    },
    Upload {
      @Override
      S3Operation build() {
        UploadOperation op = new UploadOperation();
        op.setObjectName("s3-key");
        op.setBucket("%message{s3-bucket-key}");
        op.setUserMetadataFilter(new RemoveAllMetadataFilter());
        S3ContentLanguage type = new S3ContentLanguage();
        type.setContentLanguage("english");
        op.withObjectMetadata(new S3ServerSideEncryption(), type);
        op.setCannedObjectAcl(S3ObjectCannedAcl.BUCKET_OWNER_FULL_CONTROL.name());
        return op;
      }
    },
    Copy {
      @Override
      S3Operation build() {
        return new CopyOperation()
            .withDestinationObjectName("%message{s3-dest-key}")
            .withObjectName("%message{s3-src-key}")
            .withBucket("MyS3Bucket");
      }
    },
    ExtendedCopy {
      @Override
      S3Operation build() {
        S3ContentLanguage lang = new S3ContentLanguage();
        lang.setContentLanguage("english");
        return new ExtendedCopyOperation()
            .withObjectMetadata(new ArrayList<>(Arrays.asList(new S3ServerSideEncryption(), lang)))
            .withDestinationObjectName("%message{s3-dest-key}")
            .withObjectName("%message{s3-src-key}").withBucket("MyS3Bucket");
      }
    },
    Tag {
      @Override
      S3Operation build() {
        TagOperation op = new TagOperation();
        op.setObjectName("s3-key");
        op.setBucket("%message{s3-bucket-key}");
        op.setTagMetadataFilter(new NoOpMetadataFilter());
        return op;
      }
    };
    abstract S3Operation build();
  }

  public S3ServiceTest() {
  }

  @Test
  public void testLifecycle() throws Exception {
    S3Service service = new S3Service();
    try {
      LifecycleHelper.prepare(service);
      LifecycleHelper.init(service);
      fail();
    } catch (CoreException | IllegalArgumentException expected) {

    } finally {
      LifecycleHelper.stopAndClose(service);
    }
    AmazonS3Connection connection = Mockito.mock(AmazonS3Connection.class);
    Mockito.doAnswer((i)-> {return null;}).when(connection).prepareConnection();
    Mockito.doAnswer((i)-> {return null;}).when(connection).startConnection();
    Mockito.doAnswer((i)-> {return null;}).when(connection).stopConnection();
    Mockito.doAnswer((i)-> {return null;}).when(connection).closeConnection();
    Mockito.doAnswer((i)-> {return null;}).when(connection).initConnection();
    service.setConnection(connection);
    service.setOperation(Mockito.mock(S3Operation.class));
    LifecycleHelper.initAndStart(service);
    LifecycleHelper.stopAndClose(service);
  }

  @Test
  public void testDoService() throws Exception {
    AmazonS3Connection connection = Mockito.mock(AmazonS3Connection.class);
    S3Operation operation = Mockito.mock(S3Operation.class);

    Mockito.doAnswer((i)-> {return null;}).when(connection).prepareConnection();
    Mockito.doAnswer((i)-> {return null;}).when(connection).startConnection();
    Mockito.doAnswer((i)-> {return null;}).when(connection).stopConnection();
    Mockito.doAnswer((i)-> {return null;}).when(connection).closeConnection();
    Mockito.doAnswer((i)-> {return null;}).when(connection).initConnection();
    Mockito.doAnswer((i) -> {
      return null;
    }).when(operation).execute((ClientWrapper) any(), (AdaptrisMessage) any());
    S3Service service = new S3Service(connection, operation);

    execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());
  }

  @Test
  public void testDoService_Exception() throws Exception {
    AmazonS3Connection connection = Mockito.mock(AmazonS3Connection.class);
    S3Operation operation = Mockito.mock(S3Operation.class);

    Mockito.doAnswer((i)-> {return null;}).when(connection).prepareConnection();
    Mockito.doAnswer((i)-> {return null;}).when(connection).startConnection();
    Mockito.doAnswer((i)-> {return null;}).when(connection).stopConnection();
    Mockito.doAnswer((i)-> {return null;}).when(connection).closeConnection();
    Mockito.doAnswer((i)-> {return null;}).when(connection).initConnection();
    Mockito.doThrow(new Exception()).when(operation).execute((ClientWrapper) any(),
        (AdaptrisMessage) any());
    S3Service service = new S3Service(connection, operation);

    try {
      execute(service, AdaptrisMessageFactory.getDefaultInstance().newMessage());
      fail();
    } catch (ServiceException expected) {

    }

  }

  @Override
  protected S3Service retrieveObjectForSampleConfig() {
    return null;
  }

  @Override
  protected final List retrieveObjectsForSampleConfig() {
    ArrayList result = new ArrayList();
    for (OperationsBuilder b : OperationsBuilder.values()) {
      result.add(new S3Service(new AmazonS3Connection()
          .withCredentialsProviderBuilder(new StaticCredentialsBuilder().withAuthentication(new DefaultAWSAuthentication()))
          .withClientConfiguration(exampleClientConfig()), b.build()));
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
