package org.infinispan.commons.configuration.io.xml;

import static org.junit.Assert.assertEquals;

import java.io.StringWriter;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.junit.Test;

public class XmlConfigurationWriterTest {
   @Test
   public void testEscapes() {
      StringWriter sw = new StringWriter();
      XmlConfigurationWriter w = new XmlConfigurationWriter(ConfigurationWriter.to(sw));
      w.writeStartDocument();
      w.writeStartElement("e1");
      w.writeAttribute("a1", "v\"1\"");
      w.writeAttribute("a2", "<v2>");
      w.writeEndElement(); // e1
      w.writeEndDocument();
      w.close();
      String xml = sw.toString();
      assertEquals("<?xml version=\"1.0\"?><e1 a1=\"v&#34;1&#34;\" a2=\"&#60;v2&#62;\"/>", xml);
   }
}
