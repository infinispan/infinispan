package org.infinispan.dataconversion;


import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.protostream.ProtobufUtil.toWrappedByteArray;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.io.IOException;
import java.util.Base64;

import org.infinispan.commons.dataconversion.Base16Codec;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.encoding.ProtostreamTranscoder;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.remote.impl.ProtostreamTranscoderTest")
public class ProtostreamTranscoderTest extends AbstractTranscoderTest {

   protected String dataSrc;
   private final SerializationContext ctx = createCtx();

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

   @Test
   public void testToFromObject() throws IOException {
      Person objectContent = new Person("value");
      final byte[] marshalled = toWrappedByteArray(ctx, objectContent);
      byte[] marshalledHex = Base16Codec.encode(marshalled).getBytes(UTF_8);

      // Converting from Object to protostream with different binary encodings
      Object result = transcoder.transcode(objectContent, APPLICATION_OBJECT, APPLICATION_PROTOSTREAM);
      assertArrayEquals(marshalled, (byte[]) result);

      result = transcoder.transcode(objectContent, APPLICATION_OBJECT, APPLICATION_PROTOSTREAM.withEncoding("hex"));
      assertEquals(Base16Codec.encode(marshalled), result);

      result = transcoder.transcode(objectContent, APPLICATION_OBJECT, APPLICATION_PROTOSTREAM.withEncoding("hex").withClassType(String.class));
      assertEquals(Base16Codec.encode(marshalled), result);

      // convert from protostream with different encodings back to object
      result = transcoder.transcode(marshalled, APPLICATION_PROTOSTREAM, APPLICATION_OBJECT);
      assertEquals(objectContent, result);

      result = transcoder.transcode(marshalledHex, APPLICATION_PROTOSTREAM.withEncoding("hex"), APPLICATION_OBJECT);
      assertEquals(objectContent, result);
   }

   @Test
   public void testToFromText() throws IOException {
      final String string = "This is a text";
      byte[] textContent = string.getBytes(UTF_8);

      // Converting from text/plain to protostream with different binary encodings
      Object protoWithNoEncoding = transcoder.transcode(textContent, TEXT_PLAIN, APPLICATION_PROTOSTREAM);
      assertArrayEquals(toWrappedByteArray(ctx, string), (byte[]) protoWithNoEncoding);

      Object protoHex = transcoder.transcode(textContent, TEXT_PLAIN, APPLICATION_PROTOSTREAM.withEncoding("hex"));
      assertEquals(Base16Codec.encode(toWrappedByteArray(ctx, string)), protoHex);

      Object protoBase64 = transcoder.transcode(textContent, TEXT_PLAIN, APPLICATION_PROTOSTREAM.withEncoding("base64"));
      assertEquals(Base64.getEncoder().encode(toWrappedByteArray(ctx, string)), protoBase64);

      // Converting back from protostream with different binary encodings
      Object result = transcoder.transcode(protoWithNoEncoding, APPLICATION_PROTOSTREAM, TEXT_PLAIN);
      assertArrayEquals(textContent, (byte[]) result);

   }

   @Test
   public void testToFromJson() throws IOException {
      String type = "org.infinispan.test.core.Address";
      String street = "Elm Street";
      String city = "NYC";
      int zip = 123;
      Address data = new Address(street, city, zip);
      String jsonString = Json.object()
            .set("_type", type)
            .set("street", street)
            .set("city", city)
            .set("zip", zip)
            .toString();
      byte[] byteJson = jsonString.getBytes(UTF_8);

      // Converting from json strings to protostream with different binary encodings
      Object protoWithNoEncoding = transcoder.transcode(jsonString, APPLICATION_JSON, APPLICATION_PROTOSTREAM);
      assertArrayEquals(toWrappedByteArray(ctx, data), (byte[]) protoWithNoEncoding);

      Object protoHex = transcoder.transcode(jsonString, APPLICATION_JSON, APPLICATION_PROTOSTREAM.withEncoding("hex"));
      assertEquals(protoHex, Base16Codec.encode((byte[]) protoWithNoEncoding));

      Object protoBase64 = transcoder.transcode(jsonString, APPLICATION_JSON, APPLICATION_PROTOSTREAM.withEncoding("base64"));
      assertEquals(protoBase64, Base64.getEncoder().encode((byte[]) protoWithNoEncoding));

      // Converting from json byte[] to protostream with different binary encodings
      protoWithNoEncoding = transcoder.transcode(byteJson, APPLICATION_JSON, APPLICATION_PROTOSTREAM);
      assertArrayEquals(toWrappedByteArray(ctx, data), (byte[]) protoWithNoEncoding);

      protoHex = transcoder.transcode(byteJson, APPLICATION_JSON, APPLICATION_PROTOSTREAM.withEncoding("hex"));
      assertEquals(protoHex, Base16Codec.encode((byte[]) protoWithNoEncoding));

      protoBase64 = transcoder.transcode(byteJson, APPLICATION_JSON, APPLICATION_PROTOSTREAM.withEncoding("base64"));
      assertEquals(protoBase64, Base64.getEncoder().encode((byte[]) protoWithNoEncoding));

      // Converting from protostream to json with different output binary encoding
      Object result = transcoder.transcode(protoWithNoEncoding, APPLICATION_PROTOSTREAM, APPLICATION_JSON);
      assertTrue(result instanceof byte[]);
      assertJsonCorrect(result);

      result = transcoder.transcode(protoHex, APPLICATION_PROTOSTREAM.withEncoding("hex"), APPLICATION_JSON);
      assertTrue(result instanceof byte[]);
      assertJsonCorrect(result);

      result = transcoder.transcode(protoBase64, APPLICATION_PROTOSTREAM.withEncoding("base64"), APPLICATION_JSON);
      assertTrue(result instanceof byte[]);
      assertJsonCorrect(result);

      result = transcoder.transcode(protoHex, APPLICATION_PROTOSTREAM.withEncoding("hex"), APPLICATION_JSON.withClassType(String.class));
      assertTrue(result instanceof String);
      assertJsonCorrect(result);

   }

   private void assertJsonCorrect(Object json) {
      String strJson = json instanceof byte[] ? new String((byte[]) json) : json.toString();
      Json jsonResult = Json.read(strJson);
      assertEquals("org.infinispan.test.core.Address", jsonResult.at("_type").asString());
   }
}
