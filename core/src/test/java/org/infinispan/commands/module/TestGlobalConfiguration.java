package org.infinispan.commands.module;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.CollectionAttributeCopier;
import org.infinispan.configuration.serializing.SerializedWith;

/**
 * An configuration for tests that want to register global components before the manager is created.
 *
 * @author Dan Berindei
 * @since 9.4
 */
@SerializedWith(TestGlobalConfigurationSerializer.class)
@BuiltBy(TestGlobalConfigurationBuilder.class)
public class TestGlobalConfiguration {

   public static final AttributeDefinition<Map<String, Object>> TEST_COMPONENTS =
      AttributeDefinition.<Map<String, Object>>builder("testComponents", new HashMap<>())
         .initializer(HashMap::new)
         .copier(CollectionAttributeCopier.INSTANCE).build();

   private final AttributeSet attributes;

   TestGlobalConfiguration(AttributeSet attributeSet) {
      this.attributes = attributeSet.checkProtection();
   }

   static AttributeSet attributeSet() {
      return new AttributeSet(TestGlobalConfiguration.class, TEST_COMPONENTS);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public Map<String, Object> getComponents() {
      return attributes.attribute(TEST_COMPONENTS).get();
   }

   @Override
   public String toString() {
      return "TestGlobalConfiguration [attributes=" + attributes + ']';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TestGlobalConfiguration that = (TestGlobalConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }
}
