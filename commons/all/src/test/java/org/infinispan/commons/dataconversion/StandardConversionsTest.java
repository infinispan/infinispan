package org.infinispan.commons.dataconversion;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class StandardConversionsTest {

   @Rule
   public ExpectedException thrown = ExpectedException.none();

   @Test
   public void textToTextConversion() {
      String source = "All those moments will be lost in time, like tears in rain.";
      byte[] sourceAs8859 = source.getBytes(ISO_8859_1);
      byte[] sourceAsASCII = source.getBytes(US_ASCII);

      Object result = StandardConversions.convertTextToText(sourceAs8859,
            TEXT_PLAIN.withCharset(ISO_8859_1),
            TEXT_PLAIN.withCharset(US_ASCII));

      assertArrayEquals(sourceAsASCII, (byte[]) result);
   }

   @Test
   public void testTextToObjectConversion() {
      String source = "Can the maker repair what he makes?";
      String source2 = "I had your job once. I was good at it.";

      byte[] sourceBytes = source2.getBytes(US_ASCII);

      Object result = StandardConversions.convertTextToObject(source, APPLICATION_OBJECT);
      Object result2 = StandardConversions.convertTextToObject(sourceBytes, TEXT_PLAIN.withCharset(US_ASCII));

      assertEquals(source, result);
      assertEquals(source2, result2);
   }


}
