package com.adaptris.aws.s3;

import javax.validation.constraints.NotBlank;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.interlok.util.Args;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Operation on a single S3 Object.
 *
 * <p>
 * This was introduced to demarcate operations that do work on the bucket as a whole as opposed to a
 * single S3 Object in the bucket.
 * </p>
 *
 * @since 3.10.2
 */
@NoArgsConstructor
@SuppressWarnings("deprecation")
public abstract class ObjectOperationImpl extends S3OperationImpl {

  /**
   * The Object in S3 that this operation will target.
   * <p>
   * Generally this is the full key to the object in the bucket.
   * </p>
   */
  @Getter
  @Setter
  @InputFieldHint(expression = true)
  @NotBlank
  private String objectName;


  @Override
  public void prepare() throws CoreException {
    super.prepare();
    Args.notBlank(getObjectName(), "object-name");
  }

  public <T extends ObjectOperationImpl> T withObjectName(String b) {
    setObjectName(b);
    return (T) this;
  }

  /**
   * Get the key representing the S3 Object.
   *
   */
  protected String s3ObjectKey(AdaptrisMessage msg) {
    return msg.resolve(getObjectName(), true);
  }
}
