package org.infinispan.dataconversion;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Serializable;

import org.infinispan.commons.dataconversion.DefaultTranscoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests that DefaultTranscoder uses named marshallers specified in MediaType.
 *
 * @author William Burns
 * @since 16.2
 */
@Test(groups = "functional", testName = "dataconversion.NamedMarshallerTranscoderTest")
public class NamedMarshallerTranscoderTest extends SingleCacheManagerTest {

   public static class TestObject implements Serializable {
      private static final long serialVersionUID = 1L;
      private final String value;

      public TestObject(String value) {
         this.value = value;
      }

      public String getValue() {
         return value;
      }

      @Override
      public String toString() {
         return "TestObject{value='" + value + "'}";
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();

      // Add named marshallers
      gcb.serialization()
            .addNamedMarshaller("javaMarshaller", JavaSerializationMarshaller.class.getName())
            .addNamedMarshaller("prefixMarshaller", PrefixMarshaller.class.getName())
            .allowList()
            .addClass(TestObject.class.getName());

      return TestCacheManagerFactory.createCacheManager(gcb, new ConfigurationBuilder());
   }

   public void testDefaultTranscoderUsesNamedMarshaller() throws Exception {
      EncoderRegistry encoderRegistry = TestingUtil.extractGlobalComponent(cacheManager, EncoderRegistry.class);
      DefaultTranscoder transcoder = (DefaultTranscoder) encoderRegistry.getTranscoder(DefaultTranscoder.class);
      assertNotNull("DefaultTranscoder should be registered", transcoder);

      TestObject testObject = new TestObject("test-value");

      // Create MediaType with marshaller parameter
      MediaType sourceType = MediaType.APPLICATION_OBJECT;
      MediaType targetType = MediaType.APPLICATION_OCTET_STREAM.withMarshaller("javaMarshaller");

      // Marshall using named marshaller
      Object marshalled = transcoder.transcode(testObject, sourceType, targetType);
      assertNotNull("Marshalled result should not be null", marshalled);
      assertTrue("Marshalled result should be SerializedObjectWrapper",
            marshalled instanceof org.infinispan.commons.marshall.SerializedObjectWrapper);
      byte[] marshalledBytes = ((org.infinispan.commons.marshall.SerializedObjectWrapper) marshalled).getBytes();
      assertTrue("Marshalled bytes should not be empty", marshalledBytes.length > 0);

      // Unmarshall using the same named marshaller
      MediaType sourceType2 = MediaType.APPLICATION_OCTET_STREAM.withMarshaller("javaMarshaller");
      MediaType targetType2 = MediaType.APPLICATION_OBJECT.withClassType(TestObject.class);

      Object unmarshalled = transcoder.transcode(marshalled, sourceType2, targetType2);
      assertNotNull("Unmarshalled object should not be null", unmarshalled);
      assertTrue("Unmarshalled object should be TestObject", unmarshalled instanceof TestObject);
      assertEquals("Unmarshalled value should match", "test-value", ((TestObject) unmarshalled).getValue());
   }

   public void testDefaultTranscoderFallsBackToDefaultMarshaller() throws Exception {
      EncoderRegistry encoderRegistry = TestingUtil.extractGlobalComponent(cacheManager, EncoderRegistry.class);
      DefaultTranscoder transcoder = (DefaultTranscoder) encoderRegistry.getTranscoder(DefaultTranscoder.class);
      assertNotNull("DefaultTranscoder should be registered", transcoder);

      String testString = "default-test";

      // Create MediaType without marshaller parameter - should use default
      MediaType sourceType = MediaType.APPLICATION_OBJECT;
      MediaType targetType = MediaType.APPLICATION_OCTET_STREAM;

      // Marshall using default marshaller
      Object marshalled = transcoder.transcode(testString, sourceType, targetType);
      assertNotNull("Marshalled result should not be null", marshalled);
      assertTrue("Marshalled result should be SerializedObjectWrapper",
            marshalled instanceof org.infinispan.commons.marshall.SerializedObjectWrapper);

      // Transcode back to object to verify round-trip
      Object unmarshalled = transcoder.transcode(marshalled, targetType, sourceType);
      assertEquals("String should match after round-trip", testString, unmarshalled);
   }

   public void testMediaTypeWithMarshallerParameter() {
      MediaType mediaType = MediaType.APPLICATION_OCTET_STREAM.withMarshaller("customMarshaller");
      assertEquals("Marshaller parameter should be set", "customMarshaller", mediaType.getMarshaller());

      MediaType mediaType2 = MediaType.APPLICATION_OBJECT;
      assertEquals("Marshaller parameter should be null", null, mediaType2.getMarshaller());
   }

   public void testReMarshallingBetweenDifferentMarshallers() throws Exception {
      EncoderRegistry encoderRegistry = TestingUtil.extractGlobalComponent(cacheManager, EncoderRegistry.class);
      DefaultTranscoder transcoder = (DefaultTranscoder) encoderRegistry.getTranscoder(DefaultTranscoder.class);
      assertNotNull("DefaultTranscoder should be registered", transcoder);

      TestObject testObject = new TestObject("re-marshal-test");

      // First, marshal using javaMarshaller
      MediaType sourceType = MediaType.APPLICATION_OBJECT;
      MediaType targetType = MediaType.APPLICATION_OCTET_STREAM.withMarshaller("javaMarshaller");

      Object javaMarshalled = transcoder.transcode(testObject, sourceType, targetType);
      assertNotNull("Java marshalled result should not be null", javaMarshalled);
      assertTrue("Java marshalled result should be SerializedObjectWrapper",
            javaMarshalled instanceof org.infinispan.commons.marshall.SerializedObjectWrapper);
      byte[] javaMarshalledBytes = ((org.infinispan.commons.marshall.SerializedObjectWrapper) javaMarshalled).getBytes();
      assertTrue("Java marshalled bytes should not be empty", javaMarshalledBytes.length > 0);

      // Re-marshal from javaMarshaller format to prefixMarshaller format (octet-stream to octet-stream)
      MediaType sourceType2 = MediaType.APPLICATION_OCTET_STREAM.withMarshaller("javaMarshaller");
      MediaType targetType2 = MediaType.APPLICATION_OCTET_STREAM.withMarshaller("prefixMarshaller");

      Object prefixMarshalled = transcoder.transcode(javaMarshalled, sourceType2, targetType2);
      assertNotNull("Prefix marshalled result should not be null", prefixMarshalled);
      assertTrue("Prefix marshalled result should be SerializedObjectWrapper",
            prefixMarshalled instanceof org.infinispan.commons.marshall.SerializedObjectWrapper);
      byte[] prefixMarshalledBytes = ((org.infinispan.commons.marshall.SerializedObjectWrapper) prefixMarshalled).getBytes();
      assertTrue("Prefix marshalled bytes should not be empty", prefixMarshalledBytes.length > 0);

      // Verify the bytes are different (different marshaller format)
      assertTrue("Bytes should be different after re-marshalling with different marshaller",
            !java.util.Arrays.equals(javaMarshalledBytes, prefixMarshalledBytes));

      // Verify we can unmarshal the re-marshalled bytes using the new marshaller
      MediaType sourceType3 = MediaType.APPLICATION_OCTET_STREAM.withMarshaller("prefixMarshaller");
      MediaType targetType3 = MediaType.APPLICATION_OBJECT.withClassType(TestObject.class);

      Object unmarshalled = transcoder.transcode(prefixMarshalled, sourceType3, targetType3);
      assertNotNull("Unmarshalled object should not be null", unmarshalled);
      assertTrue("Unmarshalled object should be TestObject", unmarshalled instanceof TestObject);
      assertEquals("Unmarshalled value should match original", "re-marshal-test", ((TestObject) unmarshalled).getValue());
   }
}
