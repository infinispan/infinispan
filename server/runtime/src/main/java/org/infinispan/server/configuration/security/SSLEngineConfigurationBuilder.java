package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class SSLEngineConfigurationBuilder implements Builder<SSLEngineConfiguration> {
   private final AttributeSet attributes;
   private final RealmConfigurationBuilder realmBuilder;

   SSLEngineConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
      attributes = SSLEngineConfiguration.attributeDefinitionSet();
   }

   public SSLEngineConfigurationBuilder enabledProtocols(String[] protocols) {
      attributes.attribute(SSLEngineConfiguration.ENABLED_PROTOCOLS).set(protocols);
      return this;
   }

   public SSLEngineConfigurationBuilder enabledCiphersuitesFilter(String cipherSuitesFilter) {
      attributes.attribute(SSLEngineConfiguration.ENABLED_CIPHERSUITES).set(cipherSuitesFilter);
      return this;
   }

   public SSLEngineConfigurationBuilder enabledCiphersuitesNames(String cipherSuitesNames) {
      attributes.attribute(SSLEngineConfiguration.ENABLED_CIPHERSUITES_13).set(cipherSuitesNames);
      return this;
   }

   @Override
   public SSLEngineConfiguration create() {
      return new SSLEngineConfiguration(attributes.protect());
   }

   @Override
   public SSLEngineConfigurationBuilder read(SSLEngineConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
