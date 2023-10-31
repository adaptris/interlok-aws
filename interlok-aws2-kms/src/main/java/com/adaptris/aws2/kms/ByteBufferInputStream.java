package com.adaptris.aws2.kms;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
  private final ByteBuffer wrappedBuffer;

  public ByteBufferInputStream(ByteBuffer buf) {
    wrappedBuffer = buf;
  }

  @Override
  public int read() {
    if (!wrappedBuffer.hasRemaining()) {
      return -1;
    }
    return wrappedBuffer.get() & 0xFF;
  }

  @Override
  public int read(byte[] bytes, int off, int len) {
    if (!wrappedBuffer.hasRemaining()) {
      return -1;
    }
    len = Math.min(len, wrappedBuffer.remaining());
    wrappedBuffer.get(bytes, off, len);
    return len;
  }
}
