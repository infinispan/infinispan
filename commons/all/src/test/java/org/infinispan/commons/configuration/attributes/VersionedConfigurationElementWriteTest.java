package org.infinispan.commons.configuration.attributes;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringWriter;

import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.junit.jupiter.api.Test;

public class VersionedConfigurationElementWriteTest {

   @Test
   public void testWriteOmitsElementNewerThanTargetVersion() {
      AttributeDefinition<String> value = AttributeDefinition.builder("value", "").build();
      AttributeSet attributes = new AttributeSet("test-attr", value);
      attributes.attribute(value).set("v");
      TestElement hotKeys = new TestElement("test-attr", 16, 3, attributes.protect());

      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).targetVersion(new TestConfigurationSchemaVersion(16, 2)).build()) {
         writer.writeStartDocument();
         writer.writeStartElement("root");
         hotKeys.write(writer);
         writer.writeEndElement();
         writer.writeEndDocument();
      }

      assertThat(sw.toString()).doesNotContain("test-attr");
   }

   @Test
   public void testRepeatedGroupGatedOutDoesNotOpenEmptyWrapper() {
      AttributeDefinition<String> legacyValue = AttributeDefinition.builder("value", "").build();
      AttributeSet legacyAttributes = new AttributeSet("legacy", legacyValue);
      legacyAttributes.attribute(legacyValue).set("v");
      TestElement legacyChild = new TestElement("legacy-child", legacyAttributes.protect());

      AttributeDefinition<String> entryValue = AttributeDefinition.builder("value", "").build();
      AttributeSet entryAttributes1 = new AttributeSet("entry", entryValue);
      entryAttributes1.attribute(entryValue).set("e1");
      TestElement entry1 = new TestElement("entry", true, 16, 3, entryAttributes1.protect());

      AttributeSet entryAttributes2 = new AttributeSet("entry", entryValue);
      entryAttributes2.attribute(entryValue).set("e2");
      TestElement entry2 = new TestElement("entry", true, 16, 3, entryAttributes2.protect());

      TestElement container = new TestElement("container", AttributeSet.EMPTY, legacyChild, entry1, entry2);

      // Asserts that an outer older container, with newer version children, won't include open tags for the new children.
      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).targetVersion(new TestConfigurationSchemaVersion(16, 2)).build()) {
         writer.writeStartDocument();
         container.write(writer);
         writer.writeEndDocument();
      }

      String xml = sw.toString();
      assertThat(xml).contains("legacy-child");
      assertThat(xml).doesNotContain("entry");
   }

   @Test
   public void testHomogeneousArrayGatedOutEmitsNothing() {
      AttributeDefinition<String> itemValue = AttributeDefinition.builder("value", "").build();

      AttributeSet itemAttributes1 = new AttributeSet("item", itemValue);
      itemAttributes1.attribute(itemValue).set("i1");
      TestElement item1 = new TestElement("item", 16, 3, itemAttributes1.protect());

      AttributeSet itemAttributes2 = new AttributeSet("item", itemValue);
      itemAttributes2.attribute(itemValue).set("i2");
      TestElement item2 = new TestElement("item", 16, 3, itemAttributes2.protect());

      TestElement container = new TestElement("items", AttributeSet.EMPTY, item1, item2);

      // Asserts that an older container elements that holds only newer children, will not even be included if skipped.
      // Otherwise, it would create something like <items></items>, still valid XML but not meaningful.
      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).targetVersion(new TestConfigurationSchemaVersion(16, 2)).build()) {
         writer.writeStartDocument();
         container.write(writer);
         writer.writeEndDocument();
      }

      assertThat(sw.toString()).doesNotContain("items");
   }

   @Test
   public void testAttributeAsElementOmitsAttributeNewerThanTargetVersion() {
      AttributeDefinition<String> newAttribute = AttributeDefinition.builder("new-attribute", "").since(16, 3).build();
      Attribute<String> attribute = newAttribute.toAttribute();
      attribute.set("v");
      ConfigurationElement<?> element = ConfigurationElement.child(attribute);

      // Some parts of the serialisation happens "manually" going over the elements/attributes.
      // We check that even in these cases, the newer values are also skipped.
      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).targetVersion(new TestConfigurationSchemaVersion(16, 2)).build()) {
         writer.writeStartDocument();
         writer.writeStartElement("root");
         element.write(writer);
         writer.writeEndElement();
         writer.writeEndDocument();
      }

      System.out.println(sw);
      assertThat(sw.toString()).doesNotContain("new-attribute");
   }

   private static class TestElement extends ConfigurationElement<TestElement> {
      TestElement(String element, AttributeSet attributes, ConfigurationElement<?>... children) {
         super(element, attributes, children);
      }

      TestElement(String element, int sinceMajor, int sinceMinor, AttributeSet attributes, ConfigurationElement<?>... children) {
         super(element, false, sinceMajor, sinceMinor, attributes, children);
      }

      TestElement(String element, boolean repeated, int sinceMajor, int sinceMinor, AttributeSet attributes, ConfigurationElement<?>... children) {
         super(element, repeated, sinceMajor, sinceMinor, attributes, children);
      }
   }
}
