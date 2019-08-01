package org.infinispan.persistence.rest.configuration;


import static org.infinispan.persistence.rest.configuration.RemoteServerConfiguration.HOST;
import static org.infinispan.persistence.rest.configuration.RemoteServerConfiguration.PORT;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.persistence.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @since 10.0
 */
public class RemoteServerConfigurationBuilder implements ConfigurationBuilderInfo, Builder<RemoteServerConfiguration> {

   private static final Log log = LogFactory.getLog(RemoteServerConfigurationBuilder.class, Log.class);

   private final AttributeSet attributes;

   public RemoteServerConfigurationBuilder() {
      this.attributes = RemoteServerConfiguration.attributeDefinitionSet();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return RemoteServerConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public RemoteServerConfigurationBuilder host(String host) {
      attributes.attribute(HOST).set(host);
      return this;
   }

   public RemoteServerConfigurationBuilder port(int port) {
      attributes.attribute(PORT).set(port);
      return this;
   }

   @Override
   public RemoteServerConfiguration create() {
      return new RemoteServerConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(RemoteServerConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public void validate() {
      if (attributes.attribute(HOST).get() == null) {
         throw log.hostNotSpecified();
      }
   }
}
