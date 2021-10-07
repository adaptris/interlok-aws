package com.adaptris.aws2.kms;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static com.adaptris.aws2.kms.LocalstackHelper.MSG_CONTENTS;

public class ByteBufferInputStreamTest {

  @Test
  public void testByteBufferInputStream() throws Exception {
    ByteBuffer buf = ByteBuffer.wrap(MSG_CONTENTS.getBytes(StandardCharsets.UTF_8));
    try (ByteBufferInputStream in = new ByteBufferInputStream(buf)) {
      int i = in.read();
      while (i != -1) {
        i = in.read();
      }
    }
    try (ByteBufferInputStream in = new ByteBufferInputStream(buf)) {
      byte[] bytes = IOUtils.toByteArray(in);
    }
  }
}
