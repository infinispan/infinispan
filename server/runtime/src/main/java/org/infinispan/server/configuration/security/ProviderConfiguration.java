package org.infinispan.server.configuration.security;

import java.security.Provider;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.Util;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 14.0
 **/
public class ProviderConfiguration extends ConfigurationElement<CredentialStoresConfiguration> {
   public static final AttributeDefinition<String> CLASS_NAME = AttributeDefinition.builder(Attribute.CLASS_NAME, null, String.class).build();
   public static final AttributeDefinition<String> CONFIGURATION = AttributeDefinition.builder(Attribute.CONFIGURATION, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ProviderConfiguration.class, CLASS_NAME, CONFIGURATION);
   }

   ProviderConfiguration(AttributeSet attributes) {
      super(Element.PROVIDER, attributes);
      String classname = attributes.attribute(CLASS_NAME).get();
      Object p = Util.getInstance(classname, Thread.currentThread().getContextClassLoader());
      if (p instanceof Provider) {
         Provider provider = (Provider) p;
         String configuration = attributes.attribute(CONFIGURATION).get();
         if (configuration != null) {
            provider = provider.configure(configuration);
         }
         SecurityActions.addSecurityProvider(provider);
      } else {
         throw Server.log.invalidProviderClass(classname);
      }
   }
}
