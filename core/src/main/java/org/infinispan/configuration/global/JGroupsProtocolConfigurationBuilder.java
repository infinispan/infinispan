package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.JGroupsProtocolConfiguration.PROTOCOL_CONFIG;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.jgroups.conf.ProtocolConfiguration;

/**
 * @since 10.0
 */
public class JGroupsProtocolConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<JGroupsProtocolConfiguration> {
   private final AttributeSet attributes;

   JGroupsProtocolConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = JGroupsProtocolConfiguration.attributeDefinitionSet();

   }

   public JGroupsProtocolConfigurationBuilder protocolConfig(ProtocolConfiguration protocolConfiguration) {
      attributes.attribute(PROTOCOL_CONFIG).set(protocolConfiguration);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public JGroupsProtocolConfiguration create() {
      return new JGroupsProtocolConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(JGroupsProtocolConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
