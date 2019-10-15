package org.infinispan.server.core.dataconversion;


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.test.dataconversion.AbstractTranscoderTest;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.remote.impl.ProtostreamTextTranscoderTest")
public class ProtostreamTextTranscoderTest extends AbstractTranscoderTest {

   protected String dataSrc;

   @BeforeClass(alwaysRun = true)
   public void setUp() {
      dataSrc = " !\"#$%&\'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
      SerializationContextRegistry registry = Mockito.mock(SerializationContextRegistry.class);
      Mockito.when(registry.getGlobalCtx()).thenReturn(ProtobufUtil.newSerializationContext());
      transcoder = new ProtostreamTranscoder(registry, ProtostreamTextTranscoderTest.class.getClassLoader());
      supportedMediaTypes = transcoder.getSupportedMediaTypes();
   }

   @Test
   @Override
   public void testTranscoderTranscode() throws Exception {
      Object transcoded = transcoder.transcode(dataSrc, MediaType.TEXT_PLAIN, MediaType.APPLICATION_PROTOSTREAM);
      assertTrue(transcoded instanceof byte[], "Must be byte[]");

      Object transcodedBack = transcoder.transcode(transcoded, MediaType.APPLICATION_PROTOSTREAM, MediaType.TEXT_PLAIN);

      // Must be String as byte[] as sent over the wire by hotrod
      assertTrue(transcodedBack instanceof byte[], "Must be instance of byte[]");
      assertEquals(dataSrc, new String((byte[]) transcodedBack, MediaType.TEXT_PLAIN.getCharset().name()), "Must be equal strings");
   }
}
