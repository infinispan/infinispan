package org.infinispan.configuration.global;

import static org.infinispan.configuration.parsing.Element.WHITE_LIST;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

@BuiltBy(WhiteListConfigurationBuilder.class)
public class WhiteListConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<Set<String>> CLASSES = AttributeDefinition.builder("classes", new HashSet<>(), (Class<Set<String>>) (Class<?>) Set.class)
         .initializer(HashSet::new).immutable().build();
   static final AttributeDefinition<List<String>> REGEXPS = AttributeDefinition.builder("regexps", new ArrayList<>(), (Class<List<String>>) (Class<?>) List.class)
         .initializer(ArrayList::new).immutable().build();

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(WHITE_LIST.getLocalName());


   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(WhiteListConfiguration.class, CLASSES, REGEXPS);
   }

   private final AttributeSet attributes;

   WhiteListConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public ClassWhiteList create() {
      return new ClassWhiteList(attributes.attribute(CLASSES).get(), attributes.attribute(REGEXPS).get());
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

      WhiteListConfiguration that = (WhiteListConfiguration) o;
      return Objects.equals(attributes, that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "WhiteListConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
