package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.TrustStoreConfiguration.TRUSTSTORE_FILENAME;
import static org.infinispan.persistence.remote.configuration.TrustStoreConfiguration.TRUSTSTORE_PASSWORD;
import static org.infinispan.persistence.remote.configuration.TrustStoreConfiguration.TRUSTSTORE_TYPE;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
public class TrustStoreConfigurationBuilder extends AbstractSecurityConfigurationChildBuilder implements Builder<TrustStoreConfiguration>, ConfigurationBuilderInfo {
   private static final Log log = LogFactory.getLog(TrustStoreConfigurationBuilder.class);

   protected TrustStoreConfigurationBuilder(SecurityConfigurationBuilder builder) {
      super(builder, TrustStoreConfiguration.attributeDefinitionSet());
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return TrustStoreConfiguration.ELEMENT_DEFINITION;
   }

   public TrustStoreConfigurationBuilder trustStoreFileName(String trustStoreFileName) {
      this.attributes.attribute(TRUSTSTORE_FILENAME).set(trustStoreFileName);
      return this;
   }

   public TrustStoreConfigurationBuilder trustStoreType(String trustStoreType) {
      this.attributes.attribute(TRUSTSTORE_TYPE).set(trustStoreType);
      return this;
   }

   public TrustStoreConfigurationBuilder trustStorePassword(char[] trustStorePassword) {
      this.attributes.attribute(TRUSTSTORE_PASSWORD).set(new String(trustStorePassword));
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public TrustStoreConfiguration create() {
      return new TrustStoreConfiguration(attributes.protect());
   }

   @Override
   public TrustStoreConfigurationBuilder read(TrustStoreConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

}
