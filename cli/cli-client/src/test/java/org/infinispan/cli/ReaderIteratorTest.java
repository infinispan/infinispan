package org.infinispan.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringReader;

import org.infinispan.cli.util.JsonReaderIterator;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ReaderIteratorTest {

   @Test
   public void testStructuredJSONIterator() throws IOException {
      String json = "[\n" +
            "{\n" +
            "   \"_type\": \"string\",\n" +
            "   \"_value\": \"lukecage\"\n" +
            "}\n" +
            ",\n" +
            "{\n" +
            "   \"_type\": \"string\",\n" +
            "   \"_value\": \"dannyrandy\"\n" +
            "}\n" +
            "]";
      StringReader r = new StringReader(json);
      JsonReaderIterator iterator = new JsonReaderIterator(r, (s) -> s == null || "_value".equals(s));
      assertTrue(iterator.hasNext());
      assertEquals("lukecage", iterator.next());
      assertTrue(iterator.hasNext());
      assertEquals("dannyrandy", iterator.next());
   }

   @Test
   public void testUnstructuredJSONIterator() throws IOException {
      String json = "[\"person2.proto\",\"person.proto\"]";
      StringReader r = new StringReader(json);

      JsonReaderIterator iterator = new JsonReaderIterator(r, (s) -> s == null || "_value".equals(s));
      assertTrue(iterator.hasNext());
      assertEquals("person2.proto", iterator.next());
      assertTrue(iterator.hasNext());
      assertEquals("person.proto", iterator.next());
   }
}
