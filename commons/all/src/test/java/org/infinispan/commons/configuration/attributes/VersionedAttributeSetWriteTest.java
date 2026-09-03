package org.infinispan.commons.configuration.attributes;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringWriter;

import org.infinispan.commons.configuration.io.ConfigurationSchemaVersion;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.junit.jupiter.api.Test;

public class VersionedAttributeSetWriteTest {

   @Test
   public void testWriteOmitsAttributeNewerThanTargetVersion() {
      AttributeDefinition<String> legacy = AttributeDefinition.builder("legacy", "").build();
      AttributeDefinition<String> newAttribute = AttributeDefinition.builder("new-attribute", "").since(16, 3).build();
      AttributeSet set = new AttributeSet(getClass(), legacy, newAttribute);
      set.attribute(legacy).set("a-value");
      set.attribute(newAttribute).set("b-value");

      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).targetVersion(schema(16, 2)).build()) {
         writer.writeStartDocument();
         writer.writeStartElement("root");
         set.write(writer);
         writer.writeEndElement();
         writer.writeEndDocument();
      }

      String xml = sw.toString();
      assertThat(xml).contains("legacy=\"a-value\"");
      assertThat(xml).doesNotContain("new-attribute");
   }

   @Test
   public void testDefsOverloadOmitsWrapperWhenAllDefsGatedOut() {
      AttributeDefinition<String> newAttribute = AttributeDefinition.builder("new-attribute", "").since(16, 3).build();
      AttributeSet set = new AttributeSet(getClass(), newAttribute);
      set.attribute(newAttribute).set("b-value");

      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).targetVersion(schema(16, 2)).build()) {
         writer.writeStartDocument();
         writer.writeStartElement("root");
         set.write(writer, "wrapper", newAttribute);
         writer.writeEndElement();
         writer.writeEndDocument();
      }

      assertThat(sw.toString()).doesNotContain("wrapper");
   }

   @Test
   public void testSingleAttributeOverloadOmitsAttributeNewerThanTargetVersion() {
      AttributeDefinition<String> newAttribute = AttributeDefinition.builder("new-attribute", "").since(16, 3).build();
      AttributeSet set = new AttributeSet(getClass(), newAttribute);
      set.attribute(newAttribute).set("b-value");

      StringWriter sw = new StringWriter();
      try (ConfigurationWriter writer = ConfigurationWriter.to(sw).targetVersion(schema(16, 2)).build()) {
         writer.writeStartDocument();
         writer.writeStartElement("root");
         set.write(writer, newAttribute);
         writer.writeEndElement();
         writer.writeEndDocument();
      }

      assertThat(sw.toString()).doesNotContain("new-attribute");
   }

   private static ConfigurationSchemaVersion schema(int major, int minor) {
      return new TestConfigurationSchemaVersion(major, minor);
   }
}
