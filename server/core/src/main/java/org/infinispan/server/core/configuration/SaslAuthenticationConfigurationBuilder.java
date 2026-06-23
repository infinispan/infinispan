package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * AuthenticationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class SaslAuthenticationConfigurationBuilder implements AuthenticationConfigurationBuilder<SaslAuthenticationConfiguration> {
   private final AttributeSet attributes;
   private final SaslConfigurationBuilder sasl = new SaslConfigurationBuilder();

   public SaslAuthenticationConfigurationBuilder(ProtocolServerConfigurationChildBuilder<?,?,?> builder) {
      this.attributes = SaslAuthenticationConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public SaslAuthenticationConfigurationBuilder enable() {
      return enabled(true);
   }

   public SaslAuthenticationConfigurationBuilder disable() {
      return enabled(false);
   }

   public SaslAuthenticationConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(SaslAuthenticationConfiguration.ENABLED).set(enabled);
      return this;
   }

   public boolean enabled() {
      return attributes.attribute(SaslAuthenticationConfiguration.ENABLED).get();
   }

   public SaslAuthenticationConfigurationBuilder securityRealm(String name) {
      attributes.attribute(SaslAuthenticationConfiguration.SECURITY_REALM).set(name);
      return this;
   }

   public String securityRealm() {
      return attributes.attribute(SaslAuthenticationConfiguration.SECURITY_REALM).get();
   }

   public boolean hasSecurityRealm() {
      return !attributes.attribute(SaslAuthenticationConfiguration.SECURITY_REALM).isNull();
   }

   public SaslConfigurationBuilder sasl() {
      return sasl;
   }

   @Override
   public void validate() {
      if (enabled()) {
         sasl.validate();
      }
   }

   @Override
   public SaslAuthenticationConfiguration create() {
      return new SaslAuthenticationConfiguration(attributes.protect(), sasl.create());
   }

   @Override
   public Builder<?> read(SaslAuthenticationConfiguration template, Combine combine) {
      this.sasl.read(template.sasl(), combine);
      return this;
   }
}
