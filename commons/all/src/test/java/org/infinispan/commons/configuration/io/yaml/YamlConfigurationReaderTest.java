package org.infinispan.commons.configuration.io.yaml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Properties;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationReaderException;
import org.infinispan.commons.configuration.io.NamingStrategy;
import org.infinispan.commons.configuration.io.PropertyReplacer;
import org.infinispan.commons.configuration.io.URLConfigurationResourceResolver;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Version;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class YamlConfigurationReaderTest {

   public static final String DEFAULT_NAMESPACE = "urn:infinispan:config:" + Version.getSchemaVersion();

   @Test
   public void testLine() {
      YamlConfigurationReader yaml = new YamlConfigurationReader(new StringReader(""), new URLConfigurationResourceResolver(null), new Properties(), PropertyReplacer.DEFAULT, NamingStrategy.KEBAB_CASE);
      YamlConfigurationReader.Parsed p = yaml.parseLine("    key: value");
      assertEquals(4, p.indent);
      assertEquals("key", p.name);
      assertEquals("value", p.value);
      p = yaml.parseLine("'key': 'value'");
      assertEquals(0, p.indent);
      assertEquals("key", p.name);
      assertEquals("value", p.value);
      p = yaml.parseLine("    key: value    ");
      assertEquals(4, p.indent);
      assertEquals("key", p.name);
      assertEquals("value", p.value);
      p = yaml.parseLine("  key:");
      assertEquals(2, p.indent);
      assertEquals("key", p.name);
      assertNull(p.value);
      p = yaml.parseLine("  key: # Comment");
      assertEquals(2, p.indent);
      assertEquals("key", p.name);
      assertNull(p.value);
      p = yaml.parseLine("  key: value # Comment");
      assertEquals(2, p.indent);
      assertEquals("key", p.name);
      assertEquals("value", p.value);
      p = yaml.parseLine("  # Comment");
      assertEquals(2, p.indent);
      assertNull(p.name);
      assertNull(p.value);
      p = yaml.parseLine("  - value");
      assertEquals(3, p.indent);
      assertTrue(p.listItem);
      assertNull(p.name);
      assertEquals("value", p.value);
      p = yaml.parseLine("  - key: value");
      assertEquals(3, p.indent);
      assertTrue(p.listItem);
      assertEquals("key", p.name);
      assertEquals("value", p.value);

      // Test null values
      assertNull(yaml.parseLine("key: ~").value);
      assertNull(yaml.parseLine("key: {}").value);
      assertNull(yaml.parseLine("key: null").value);
      assertNull(yaml.parseLine("key: Null").value);
      assertNull(yaml.parseLine("key: NULL").value);
      assertEquals("null", yaml.parseLine("key: \"null\"").value);
      assertEquals("nullAndSome", yaml.parseLine("key: \"nullAndSome\"").value);
      assertEquals("Null", yaml.parseLine("key: \"Null\"").value);
      assertEquals("NULL", yaml.parseLine("key: \"NULL\"").value);
      assertEquals("NUll", yaml.parseLine("key: NUll").value);

      Exceptions.expectException(ConfigurationReaderException.class, () -> yaml.parseLine("  key # comment"));
      Exceptions.expectException(ConfigurationReaderException.class, () -> yaml.parseLine("  key value"));
   }

   @Test
   public void testYamlFile() throws IOException {
      try (Reader r = new InputStreamReader(YamlConfigurationReaderTest.class.getResourceAsStream("/yaml.yaml"))) {
         Properties properties = new Properties();
         properties.put("opinion", "YAML is awful");
         properties.put("fact", "YAML is really awful");
         YamlConfigurationReader yaml = new YamlConfigurationReader(r, new URLConfigurationResourceResolver(null), properties, PropertyReplacer.DEFAULT, NamingStrategy.KEBAB_CASE);
         yaml.require(ConfigurationReader.ElementType.START_DOCUMENT);
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.START_ELEMENT, DEFAULT_NAMESPACE, "item1");
         assertEquals(2, yaml.getAttributeCount());
         assertAttribute(yaml, "item5", "v5");
         assertAttribute(yaml, "item6", "v6");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.START_ELEMENT, DEFAULT_NAMESPACE, "item2");
         assertEquals(4, yaml.getAttributeCount());
         assertAttribute(yaml, "a", "1");
         assertAttribute(yaml, "b", "2");
         assertAttribute(yaml, "c", "3");
         assertAttribute(yaml, "d", "4");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.START_ELEMENT, DEFAULT_NAMESPACE, "camel-item3");
         assertEquals("camel-attribute", yaml.getAttributeName(0));
         assertEquals("YAML is awful", yaml.getAttributeValue(0));
         assertEquals("another-camel-attribute", yaml.getAttributeName(1));
         assertEquals("YAML is really awful", yaml.getAttributeValue(1));
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.END_ELEMENT, DEFAULT_NAMESPACE, "camel-item3");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.END_ELEMENT, DEFAULT_NAMESPACE, "item2");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.START_ELEMENT, DEFAULT_NAMESPACE, "item7");
         assertEquals("v7", yaml.getElementText());
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.END_ELEMENT, DEFAULT_NAMESPACE, "item7");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.START_ELEMENT, DEFAULT_NAMESPACE, "item7");
         assertEquals("v8", yaml.getElementText());
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.END_ELEMENT, DEFAULT_NAMESPACE, "item7");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.START_ELEMENT, DEFAULT_NAMESPACE, "item8");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.START_ELEMENT, DEFAULT_NAMESPACE, "item8");
         assertEquals(2, yaml.getAttributeCount());
         assertAttribute(yaml, "a", "1");
         assertAttribute(yaml, "b", "2");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.END_ELEMENT, DEFAULT_NAMESPACE, "item8");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.START_ELEMENT, DEFAULT_NAMESPACE, "item8");
         assertEquals(2, yaml.getAttributeCount());
         assertAttribute(yaml, "a", "3");
         assertAttribute(yaml, "b", "4");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.END_ELEMENT, DEFAULT_NAMESPACE, "item8");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.END_ELEMENT, DEFAULT_NAMESPACE, "item8");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.START_ELEMENT, DEFAULT_NAMESPACE, "item9");
         assertEquals(2, yaml.getAttributeCount());
         assertAttribute(yaml, "a", "true");
         assertAttribute(yaml, "b", "1 2 3 ");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.END_ELEMENT, DEFAULT_NAMESPACE, "item9");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.END_ELEMENT, DEFAULT_NAMESPACE, "item1");
         yaml.nextElement();
         yaml.require(ConfigurationReader.ElementType.END_DOCUMENT);
      }
   }

   private void assertAttribute(YamlConfigurationReader yaml, String name, String value) {
      for (int i = 0; i < yaml.getAttributeCount(); i++) {
         if (name.equals(yaml.getAttributeName(i))) {
            assertEquals(value, yaml.getAttributeValue(i));
            return;
         }
      }
      fail("Could not find attribute '" + name + " in element '" + yaml.getLocalName() + "'");
   }
}
