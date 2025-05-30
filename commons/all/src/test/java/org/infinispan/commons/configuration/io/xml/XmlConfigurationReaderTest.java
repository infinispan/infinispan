package org.infinispan.commons.configuration.io.xml;

import static org.infinispan.commons.configuration.io.yaml.YamlConfigurationReaderTest.assertAttribute;
import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.util.Properties;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolvers;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.configuration.io.PropertyReplacer;
import org.junit.Test;

public class XmlConfigurationReaderTest {
   @Test
   public void testEscapes() {
      StringReader sr =new StringReader("""
            <e1 a1="v&#34;1&#34;" a2="&#60;v2>" a3="&quot;"/>
            """);
      try (XmlConfigurationReader r = new XmlConfigurationReader(sr, ConfigurationResourceResolvers.DEFAULT, new Properties(), PropertyReplacer.DEFAULT, NamingStrategy.CAMEL_CASE)) {
         r.require(ConfigurationReader.ElementType.START_DOCUMENT);
         r.nextElement();
         r.require(ConfigurationReader.ElementType.START_ELEMENT, "", "e1");
         assertEquals(3, r.getAttributeCount());
         assertAttribute(r, "a1", "v\"1\"");
         assertAttribute(r, "a2", "<v2>");
         assertAttribute(r, "a3", "\"");
         r.nextElement();
         r.require(ConfigurationReader.ElementType.END_ELEMENT);
         r.nextElement();
         r.require(ConfigurationReader.ElementType.END_DOCUMENT);
      }
   }

   @Test
   public void testCDATA() {
      StringReader sr =new StringReader("""
            <e1><![CDATA[<v2>]]></e1>
            """);
      try (XmlConfigurationReader r = new XmlConfigurationReader(sr, ConfigurationResourceResolvers.DEFAULT, new Properties(), PropertyReplacer.DEFAULT, NamingStrategy.CAMEL_CASE)) {
         r.require(ConfigurationReader.ElementType.START_DOCUMENT);
         r.nextElement();
         r.require(ConfigurationReader.ElementType.START_ELEMENT, "", "e1");
         assertEquals("<v2>", r.getElementText());
         r.nextElement();
      }
   }
}
