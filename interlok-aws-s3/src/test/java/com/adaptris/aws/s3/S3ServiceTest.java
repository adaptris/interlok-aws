package com.adaptris.aws.s3;

import com.adaptris.aws.s3.S3GetOperation;
import com.adaptris.aws.s3.S3Service;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.common.ConstantDataInputParameter;
import com.adaptris.core.common.PayloadStreamOutputParameter;

public class S3ServiceTest extends ServiceCase {

  public S3ServiceTest(String name) {
    super(name);
  }

  @Override
  protected S3Service retrieveObjectForSampleConfig() {
    S3GetOperation op = new S3GetOperation();
    op.setBucketName(new ConstantDataInputParameter("bucket-name"));
    op.setKey(new ConstantDataInputParameter("key"));
    op.setResponseBody(new PayloadStreamOutputParameter());
    
    S3Service s3s = new S3Service();
    s3s.setOperation(op);
    
    return s3s;
  }

}
