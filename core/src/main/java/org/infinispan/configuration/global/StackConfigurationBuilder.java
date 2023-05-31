package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.StackConfiguration.EXTENDS;
import static org.infinispan.configuration.global.StackConfiguration.NAME;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.remoting.transport.jgroups.EmbeddedJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelConfigurator;

/*
 * @since 10.0
 */
public class StackConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements StackBuilder<StackConfiguration> {
   private final AttributeSet attributes;
   private final JGroupsConfigurationBuilder jgroups;
   private EmbeddedJGroupsChannelConfigurator configurator;

   StackConfigurationBuilder(String name, JGroupsConfigurationBuilder jgroups) {
      super(jgroups.getGlobalConfig());
      this.jgroups = jgroups;
      attributes = StackConfiguration.attributeDefinitionSet();
      attributes.attribute(NAME).set(name);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public StackConfigurationBuilder extend(String extend) {
      attributes.attribute(EXTENDS).set(extend);
      return this;
   }

   public StackConfigurationBuilder channelConfigurator(EmbeddedJGroupsChannelConfigurator configurator) {
      String extend = attributes.attribute(EXTENDS).get();
      this.configurator = extend == null ? configurator :
            new EmbeddedJGroupsChannelConfigurator(
                  configurator.getName(),
                  configurator.getUncombinedProtocolStack(),
                  configurator.getUncombinedRemoteSites(),
                  extend
            );
      return this;
   }

   @Override
   public StackConfiguration create() {
      return new StackConfiguration(attributes.protect(), configurator);
   }

   @Override
   public StackConfigurationBuilder read(StackConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      this.configurator = template.configurator();
      return this;
   }

   @Override
   public JGroupsChannelConfigurator getConfigurator() {
      return configurator;
   }
}
