package org.infinispan.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import org.infinispan.cli.util.JsonReaderIterator;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ReaderIteratorTest {

   @Test
   public void testUnstructuredJSONIterator() throws IOException {
      String json = "[\"person2.proto\",\"person.proto\"]";
      StringReader r = new StringReader(json);

      JsonReaderIterator iterator = new JsonReaderIterator(r);
      assertTrue(iterator.hasNext());
      assertEquals("person2.proto", iterator.next().values().iterator().next());
      assertTrue(iterator.hasNext());
      assertEquals("person.proto", iterator.next().values().iterator().next());
   }

   @Test
   public void testStructuredJSONIterator() throws IOException {
      String json = "[{\"key\":\n" +
            "{\n" +
            "   \"_type\": \"string\",\n" +
            "   \"_value\": \"k1\"\n" +
            "}\n" +
            ",\"value\":\n" +
            "{\n" +
            "   \"_type\": \"string\",\n" +
            "   \"_value\": \"v1\"\n" +
            "}\n" +
            ",\"timeToLiveSeconds\": 12000, \"maxIdleTimeSeconds\": 12000, \"created\": 1655871119343, \"lastUsed\": 1655871119343, \"expireTime\": 1655883119343}," +
            "{\"key\":\n" +
            "{\n" +
            "   \"_type\": \"string\",\n" +
            "   \"_value\": \"k2\"\n" +
            "}\n" +
            ",\"value\":\n" +
            "{\n" +
            "   \"_type\": \"string\",\n" +
            "   \"_value\": \"v2\"\n" +
            "}\n" +
            ",\"timeToLiveSeconds\": 12000, \"maxIdleTimeSeconds\": 12000, \"created\": 1655871119343, \"lastUsed\": 1655871119343, \"expireTime\": 1655883119343}," +
            "]";
      StringReader r = new StringReader(json);
      JsonReaderIterator iterator = new JsonReaderIterator(r);
      assertTrue(iterator.hasNext());
      Map<String, String> row = iterator.next();
      assertEquals("k1", row.get("key"));
      assertEquals("v1", row.get("value"));
      assertEquals("12000", row.get("timeToLiveSeconds"));
      assertEquals("12000", row.get("maxIdleTimeSeconds"));
      assertEquals("1655871119343", row.get("created"));
      assertEquals("1655871119343", row.get("lastUsed"));
      assertEquals("1655883119343", row.get("expireTime"));
      assertTrue(iterator.hasNext());
      row = iterator.next();
      assertEquals("k2", row.get("key"));
      assertEquals("v2", row.get("value"));
      assertEquals("12000", row.get("timeToLiveSeconds"));
      assertEquals("12000", row.get("maxIdleTimeSeconds"));
      assertEquals("1655871119343", row.get("created"));
      assertEquals("1655871119343", row.get("lastUsed"));
      assertEquals("1655883119343", row.get("expireTime"));
   }
}
