package org.infinispan.commons.configuration.io.json;

import static org.junit.Assert.assertEquals;

import java.io.StringWriter;

import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class JsonConfigurationWriterTest {

   @Test
   public void testWriteJson() {
      StringWriter sw = new StringWriter();
      JsonConfigurationWriter w = new JsonConfigurationWriter(sw, false, false);
      w.writeStartDocument();
      w.writeStartElement("e1");
      w.writeAttribute("a1", "v1");
      w.writeAttribute("a2", "v2");
      w.writeEndElement(); // e1
      w.writeStartListElement("e2", true);
      w.writeStartElement("e2");
      w.writeAttribute("a3", "v3");
      w.writeAttribute("a4", "v4");
      w.writeEndElement();
      w.writeStartElement("e2");
      w.writeAttribute("a3", "v3");
      w.writeAttribute("a4", "v4");
      w.writeEndElement();
      w.writeEndElement();
      w.writeEndDocument();
      w.close();
      String json = sw.toString();
      assertEquals("{\"e1\":{\"a1\":\"v1\",\"a2\":\"v2\"},\"e2\":[{\"a3\":\"v3\",\"a4\":\"v4\"},{\"a3\":\"v3\",\"a4\":\"v4\"}]}", json);
   }
}
