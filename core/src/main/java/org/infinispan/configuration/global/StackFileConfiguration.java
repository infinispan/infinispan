package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelConfigurator;

/**
 * @since 10.0
 */
public class StackFileConfiguration extends ConfigurationElement<StackFileConfiguration> implements NamedStackConfiguration {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.NAME, null, String.class).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.PATH, null, String.class).build();
   static final AttributeDefinition<Boolean> BUILTIN = AttributeDefinition.builder("builtin", false, Boolean.class).autoPersist(false).build();
   private final JGroupsChannelConfigurator configurator;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(StackFileConfiguration.class, NAME, PATH, BUILTIN);
   }

   StackFileConfiguration(AttributeSet attributes, JGroupsChannelConfigurator configurator) {
      super(Element.STACK_FILE, attributes);
      this.configurator = configurator;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public String path() {
      return attributes.attribute(PATH).get();
   }

   public boolean builtIn() {
      return attributes.attribute(BUILTIN).get();
   }

   public JGroupsChannelConfigurator configurator() {
      return configurator;
   }
}
