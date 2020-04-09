package org.infinispan.dataconversion;


import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.encoding.ProtostreamTranscoder;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.remote.impl.ProtostreamTranscoderTest")
public class ProtostreamTranscoderTest extends AbstractTranscoderTest {

   protected String dataSrc;
   private SerializationContext ctx = createCtx();

   static final MediaType UNWRAPPED_PROTOSTREAM = APPLICATION_PROTOSTREAM.withParameter("wrapped", "false");
   static final MediaType TYPED_OBJECT = APPLICATION_OBJECT.withParameter("type", Person.class.getName());


   @BeforeClass(alwaysRun = true)
   public void setUp() {
      dataSrc = " !\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
      SerializationContextRegistry registry = Mockito.mock(SerializationContextRegistry.class);
      Mockito.when(registry.getUserCtx()).thenReturn(ctx);
      transcoder = new ProtostreamTranscoder(registry, ProtostreamTranscoderTest.class.getClassLoader());
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   private SerializationContext createCtx() {
      SerializationContext ctx = ProtobufUtil.newSerializationContext();
      TestDataSCI.INSTANCE.registerSchema(ctx);
      TestDataSCI.INSTANCE.registerMarshallers(ctx);
      return ctx;
   }


   @Test
   @Override
   public void testTranscoderTranscode() throws Exception {
      Object transcoded = transcoder.transcode(dataSrc, TEXT_PLAIN, APPLICATION_PROTOSTREAM);
      assertTrue(transcoded instanceof byte[], "Must be byte[]");

      Object transcodedBack = transcoder.transcode(transcoded, APPLICATION_PROTOSTREAM, TEXT_PLAIN);

      // Must be String as byte[] as sent over the wire by hotrod
      assertTrue(transcodedBack instanceof byte[], "Must be instance of byte[]");
      assertEquals(dataSrc, new String((byte[]) transcodedBack, TEXT_PLAIN.getCharset().name()), "Must be equal strings");
   }

   @Test
   public void testWrappedMessage() throws IOException {
      Person input = new Person("value");

      // Produces MessageWrapped and unwrapped payloads
      byte[] wrapped = (byte[]) transcoder.transcode(input, APPLICATION_OBJECT, APPLICATION_PROTOSTREAM);
      byte[] unwrapped = (byte[]) transcoder.transcode(input, APPLICATION_OBJECT, UNWRAPPED_PROTOSTREAM);

      assertEquals(input, ProtobufUtil.fromWrappedByteArray(ctx, wrapped));
      assertEquals(input, ProtobufUtil.fromByteArray(ctx, unwrapped, Person.class));

      // Convert from MessageWrapped back to object
      Object fromWrapped = transcoder.transcode(wrapped, APPLICATION_PROTOSTREAM, APPLICATION_OBJECT);
      assertEquals(input, fromWrapped);

      // Convert from unwrapped payload back to object, specifying the object type
      Object fromUnWrappedWithType = transcoder.transcode(unwrapped, UNWRAPPED_PROTOSTREAM, TYPED_OBJECT);
      assertEquals(input, fromUnWrappedWithType);

      // Should throw exception if trying to convert from unwrapped without passing the type
      try {
         transcoder.transcode(unwrapped, UNWRAPPED_PROTOSTREAM, APPLICATION_OBJECT);
         Assert.fail("should not convert from unwrapped without type");
      } catch (MarshallingException ignored) {
      }
   }
}
