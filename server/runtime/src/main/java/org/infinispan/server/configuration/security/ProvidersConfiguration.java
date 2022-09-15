package org.infinispan.server.configuration.security;

import java.util.List;
import java.util.Properties;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Element;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 14.0
 **/
public class ProvidersConfiguration extends ConfigurationElement<ProvidersConfiguration> {
   private final List<ProviderConfiguration> providers;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ProvidersConfiguration.class);
   }

   ProvidersConfiguration(AttributeSet attributes, List<ProviderConfiguration> providers, Properties properties) {
      super(Element.PROVIDERS, attributes);
      attributes.checkProtection();
      this.providers = providers;
   }

   public List<ProviderConfiguration> providers() {
      return providers;
   }
}
