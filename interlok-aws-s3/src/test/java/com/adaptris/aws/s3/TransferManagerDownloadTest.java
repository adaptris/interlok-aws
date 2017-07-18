package com.adaptris.aws.s3;

import com.adaptris.core.ServiceCase;
import com.adaptris.core.common.ConstantDataInputParameter;

@SuppressWarnings("deprecation")
public class TransferManagerDownloadTest extends ServiceCase {

  public TransferManagerDownloadTest(String name) {
    super(name);
  }

  @Override
  protected TransferManagerDownload retrieveObjectForSampleConfig() {
    TransferManagerDownload s3s = new TransferManagerDownload();
    s3s.setBucketName(new ConstantDataInputParameter("bucket-name"));
    s3s.setKey(new ConstantDataInputParameter("filename"));
    s3s.setTempDirectory("/path/to/tmp/dir");
    return s3s;
  }

}
