package org.infinispan.dataconversion;


import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.dataconversion.DefaultTranscoder;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.core.EncoderRegistryImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(groups = "functional", testName = "core.TranscoderRegistrationTest")
public class TranscoderRegistrationTest {

   public void testTranscoderLookup() {
      EncoderRegistry encoderRegistry = new EncoderRegistryImpl();
      TestTranscoder t1 = new TestTranscoder(MediaType.APPLICATION_JSON, MediaType.APPLICATION_OBJECT);
      TestTranscoder t2 = new TestTranscoder(MediaType.APPLICATION_XML, MediaType.APPLICATION_OBJECT);
      DefaultTranscoder t3 = new DefaultTranscoder(new GenericJBossMarshaller(), new JavaSerializationMarshaller());

      encoderRegistry.registerTranscoder(t3);
      encoderRegistry.registerTranscoder(t2);
      encoderRegistry.registerTranscoder(t1);

      assertEquals(encoderRegistry.getTranscoder(MediaType.TEXT_PLAIN, MediaType.APPLICATION_OBJECT), t3);
      assertEquals(encoderRegistry.getTranscoder(MediaType.TEXT_PLAIN, MediaType.TEXT_PLAIN), t3);
      assertEquals(encoderRegistry.getTranscoder(MediaType.TEXT_PLAIN, MediaType.APPLICATION_OBJECT), t3);
      assertEquals(encoderRegistry.getTranscoder(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM), t3);
      assertEquals(encoderRegistry.getTranscoder(MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_OBJECT), t3);
      assertEquals(encoderRegistry.getTranscoder(MediaType.TEXT_PLAIN, MediaType.APPLICATION_OCTET_STREAM), t3);
      assertNotFound(encoderRegistry, MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML);
      assertNotFound(encoderRegistry, MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON);


      assertEquals(encoderRegistry.getTranscoder(MediaType.APPLICATION_JSON, MediaType.APPLICATION_OBJECT), t1);
      assertEquals(encoderRegistry.getTranscoder(MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_JSON), t1);
      assertEquals(encoderRegistry.getTranscoder(MediaType.APPLICATION_XML, MediaType.APPLICATION_OBJECT), t2);
      assertEquals(encoderRegistry.getTranscoder(MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_XML), t2);
      assertEquals(encoderRegistry.getTranscoder(MediaType.APPLICATION_WWW_FORM_URLENCODED, MediaType.APPLICATION_WWW_FORM_URLENCODED), t3);

   }

   private void assertNotFound(EncoderRegistry registry, MediaType one, MediaType other) {
      try {
         registry.getTranscoder(one, other);
         Assert.fail("Should not have found transcoder");
      } catch (EncodingException ignored) {
      }
   }

   private static final class TestTranscoder implements Transcoder {

      Set<MediaType> supportedSet = new HashSet<>();

      TestTranscoder(MediaType... supported) {
         supportedSet.addAll(Arrays.asList(supported));
      }

      @Override
      public Object transcode(Object content, MediaType contentType, MediaType destinationType) {
         return content;
      }

      @Override
      public Set<MediaType> getSupportedMediaTypes() {
         return supportedSet;
      }

      @Override
      public String toString() {
         return "TestTranscoder{" +
               "supportedSet=" + supportedSet +
               '}';
      }
   }

}
