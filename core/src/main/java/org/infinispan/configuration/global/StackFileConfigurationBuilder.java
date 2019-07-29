package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.StackFileConfiguration.NAME;
import static org.infinispan.configuration.global.StackFileConfiguration.PATH;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.remoting.transport.jgroups.FileJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelConfigurator;

/*
 * @since 10.0
 */
public class StackFileConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements StackBuilder<StackFileConfiguration> {
   private final AttributeSet attributes;
   private FileJGroupsChannelConfigurator configurator;

   StackFileConfigurationBuilder(String name, GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = StackFileConfiguration.attributeDefinitionSet();
      attributes.attribute(NAME).set(name);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public StackFileConfigurationBuilder path(String name) {
      attributes.attribute(StackFileConfiguration.PATH).set(name);
      return this;
   }

   public StackFileConfigurationBuilder fileChannelConfigurator(FileJGroupsChannelConfigurator configurator) {
      this.configurator = configurator;
      attributes.attribute(NAME).set(configurator.getName());
      attributes.attribute(PATH).set(configurator.getPath());
      return this;
   }

   public JGroupsChannelConfigurator getConfigurator() {
      return configurator;
   }

   @Override
   public void validate() {
   }

   @Override
   public StackFileConfiguration create() {
      return new StackFileConfiguration(attributes.protect());
   }

   @Override
   public StackFileConfigurationBuilder read(StackFileConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "StackFileConfigurationBuilder{" +
            "attributes=" + attributes +
            '}';
   }

   @Override
   public JGroupsChannelConfigurator getConfigurator(String stackName) {
      return configurator;
   }
}
