package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.TransportSecurityConfiguration.SECURITY_REALM;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class TransportSecurityConfigurationBuilder implements Builder<TransportSecurityConfiguration> {
   private final AttributeSet attributes;

   TransportSecurityConfigurationBuilder() {
      this.attributes = TransportSecurityConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public void validate() {
   }

   @Override
   public TransportSecurityConfiguration create() {
      return new TransportSecurityConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(TransportSecurityConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   public TransportSecurityConfigurationBuilder securityRealm(String securityRealm) {
      attributes.attribute(SECURITY_REALM).set(securityRealm);
      return this;
   }
}
