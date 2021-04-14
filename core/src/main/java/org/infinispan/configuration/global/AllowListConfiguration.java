package org.infinispan.configuration.global;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

@BuiltBy(AllowListConfigurationBuilder.class)
public class AllowListConfiguration {
   static final AttributeDefinition<Set<String>> CLASSES = AttributeDefinition.builder("classes", new HashSet<>(), (Class<Set<String>>) (Class<?>) Set.class)
         .initializer(HashSet::new).immutable().build();
   static final AttributeDefinition<List<String>> REGEXPS = AttributeDefinition.builder("regexps", new ArrayList<>(), (Class<List<String>>) (Class<?>) List.class)
         .initializer(ArrayList::new).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AllowListConfiguration.class, CLASSES, REGEXPS);
   }

   private final AttributeSet attributes;
   private final ClassLoader classLoader;

   AllowListConfiguration(AttributeSet attributes, ClassLoader classLoader) {
      this.attributes = attributes.checkProtection();
      this.classLoader = classLoader;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public ClassAllowList create() {
      return new ClassAllowList(attributes.attribute(CLASSES).get(), attributes.attribute(REGEXPS).get(), classLoader);
   }

   public Set<String> getClasses() {
      return attributes.attribute(CLASSES).get();
   }

   public List<String> getRegexps() {
      return attributes.attribute(REGEXPS).get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AllowListConfiguration that = (AllowListConfiguration) o;
      return Objects.equals(attributes, that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "AllowListConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
