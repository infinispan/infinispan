package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.StackConfiguration.NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.remoting.transport.jgroups.EmbeddedJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.JGroupsChannelConfigurator;

/*
 * @since 10.0
 */
public class StackConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements StackBuilder<StackConfiguration> {
   private final AttributeSet attributes;

   private List<JGroupsProtocolConfigurationBuilder> protocols = new ArrayList<>();
   private EmbeddedJGroupsChannelConfigurator configurator;

   StackConfigurationBuilder(String name, GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = StackConfiguration.attributeDefinitionSet();
      attributes.attribute(NAME).set(name);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public JGroupsProtocolConfigurationBuilder newProtocol() {
      JGroupsProtocolConfigurationBuilder protocolConfigurationBuilder = new JGroupsProtocolConfigurationBuilder(getGlobalConfig());
      this.protocols.add(protocolConfigurationBuilder);
      return protocolConfigurationBuilder;
   }

   public StackConfigurationBuilder channelConfigurator(EmbeddedJGroupsChannelConfigurator configurator) {
      this.configurator = configurator;
      attributes.attribute(NAME).set(configurator.getName());
      configurator.getProtocolStack().forEach(protocolConfiguration -> {
         JGroupsProtocolConfigurationBuilder protocol = newProtocol();
         protocol.protocolConfig(protocolConfiguration);
      });
      return this;
   }

   @Override
   public StackConfiguration create() {
      List<JGroupsProtocolConfiguration> protocolConfigurations = protocols.stream()
            .map(JGroupsProtocolConfigurationBuilder::create).collect(Collectors.toList());
      return new StackConfiguration(attributes.protect(), protocolConfigurations);
   }

   @Override
   public StackConfigurationBuilder read(StackConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public JGroupsChannelConfigurator getConfigurator(String stackName) {
      return configurator;
   }
}
