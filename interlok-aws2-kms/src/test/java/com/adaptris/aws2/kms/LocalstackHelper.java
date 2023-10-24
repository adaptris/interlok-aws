package com.adaptris.aws2.kms;

import com.adaptris.core.util.PropertyHelper;
import com.adaptris.util.stream.DevNullOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.commons.lang3.BooleanUtils;

import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LocalstackHelper {
  public static final String TESTS_ENABLED = "localstack.tests.enabled";
  public static final String PROPERTIES_RESOURCE = "unit-tests.properties";
  public static final String KMS_SIGNING_REGION = "localstack.kms.signingRegion";
  public static final String KMS_URL = "localstack.kms.url";

  public static final Properties CONFIG = PropertyHelper.loadQuietly(PROPERTIES_RESOURCE);

  public static final String MSG_CONTENTS = "hello world";
  public static final String HASH_METADATA_KEY = "payload-hash";
  public static final String SIG_METADATA_KEY = "payload-signature";

  public static final String HASH_ALG = "SHA-256";


  public static byte[] hash(String s) throws Exception {
    MessageDigest digest = MessageDigest.getInstance(HASH_ALG);
    try (InputStream in = new ReaderInputStream(new StringReader(s), StandardCharsets.UTF_8);
        DigestOutputStream out = new DigestOutputStream(new DevNullOutputStream(), digest)) {
      IOUtils.copy(in, out);
    }
    return digest.digest();
  }

  public static byte[] hash(byte[] bytes) throws Exception {
    MessageDigest digest = MessageDigest.getInstance(HASH_ALG);
    digest.update(bytes);
    return digest.digest();
  }


  public static boolean areTestsEnabled() {
    return BooleanUtils.toBoolean(CONFIG.getProperty(TESTS_ENABLED, "false"));
  }

  public static void assertNotEqual(byte[] expected, byte[] actual) throws Exception {
    assertFalse(MessageDigest.isEqual(hash(expected), hash(actual)));
  }

  public static void assertEqual(byte[] expected, byte[] actual) throws Exception {
    assertTrue(MessageDigest.isEqual(hash(expected), hash(actual)));
  }
}
