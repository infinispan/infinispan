package org.infinispan.configuration.global;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.configuration.parsing.Element;

@BuiltBy(AllowListConfigurationBuilder.class)
public class AllowListConfiguration extends ConfigurationElement<AllowListConfiguration> {
   static final AttributeDefinition<Set<String>> CLASSES = AttributeDefinition.builder("classes", new HashSet<>(), (Class<Set<String>>) (Class<?>) Set.class)
         .initializer(HashSet::new).immutable().serializer(new AttributeSerializer<>() {
            @Override
            public void serialize(ConfigurationWriter writer, String name, Set<String> value) {
               writer.writeArrayElement(Element.CLASS, Element.CLASS, null, value);
            }

            @Override
            public boolean defer() {
               return true;
            }
         }).build();
   static final AttributeDefinition<List<String>> REGEXPS = AttributeDefinition.builder("regexps", new ArrayList<>(), (Class<List<String>>) (Class<?>) List.class)
         .initializer(ArrayList::new).immutable().serializer(new AttributeSerializer<>() {
            @Override
            public void serialize(ConfigurationWriter writer, String name, List<String> value) {
               writer.writeArrayElement(Element.REGEX, Element.REGEX, null, value);
            }

            @Override
            public boolean defer() {
               return true;
            }
         }).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(AllowListConfiguration.class, CLASSES, REGEXPS);
   }

   private final ClassLoader classLoader;

   AllowListConfiguration(AttributeSet attributes, ClassLoader classLoader) {
      super(Element.ALLOW_LIST, attributes);
      this.classLoader = classLoader;
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
}
