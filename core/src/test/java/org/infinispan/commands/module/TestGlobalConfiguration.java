package org.infinispan.commands.module;

import static org.infinispan.commons.configuration.attributes.IdentityAttributeCopier.identityCopier;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.factories.ComponentRegistry;

/**
 * An configuration for tests that want to register global components before the manager is created.
 *
 * @author Dan Berindei
 * @since 9.4
 */
@SerializedWith(TestGlobalConfigurationSerializer.class)
@BuiltBy(TestGlobalConfigurationBuilder.class)
public class TestGlobalConfiguration {

   static final AttributeDefinition<Map<String, Object>> GLOBAL_TEST_COMPONENTS =
         AttributeDefinition.<Map<String, Object>>builder("globalTestComponents", new HashMap<>())
               .initializer(HashMap::new)
               .copier(identityCopier()).build();
   static final AttributeDefinition<Map<String, Map<String, Object>>> CACHE_TEST_COMPONENTS =
         AttributeDefinition.<Map<String, Map<String, Object>>>builder("cacheTestComponents", new HashMap<>())
               .initializer(HashMap::new)
               .copier(identityCopier()).build();
   static final AttributeDefinition<Consumer<ComponentRegistry>> CACHE_STARTING_CALLBACK =
         AttributeDefinition.<Consumer<ComponentRegistry>>builder("cacheStartingCallback", cr -> {})
               .copier(identityCopier()).build();

   private final AttributeSet attributes;

   TestGlobalConfiguration(AttributeSet attributeSet) {
      this.attributes = attributeSet.checkProtection();
   }

   static AttributeSet attributeSet() {
      return new AttributeSet(TestGlobalConfiguration.class, GLOBAL_TEST_COMPONENTS, CACHE_TEST_COMPONENTS,
                              CACHE_STARTING_CALLBACK);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public Map<String, Object> globalTestComponents() {
      return attributes.attribute(GLOBAL_TEST_COMPONENTS).get();
   }

   public Map<String, Object> cacheTestComponents(String cacheName) {
      return attributes.attribute(CACHE_TEST_COMPONENTS).get().get(cacheName);
   }

   public Set<String> cacheTestComponentNames() {
      return attributes.attribute(CACHE_TEST_COMPONENTS).get().values().stream()
                       .flatMap(m -> m.keySet().stream())
                       .collect(Collectors.toSet());
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

   public Consumer<ComponentRegistry> cacheStartCallback() {
      return attributes.attribute(CACHE_STARTING_CALLBACK).get();
   }
}
