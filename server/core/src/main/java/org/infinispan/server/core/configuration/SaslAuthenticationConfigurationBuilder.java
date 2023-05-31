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
   private boolean enabled = false;
   private final SaslConfigurationBuilder sasl = new SaslConfigurationBuilder();

   public SaslAuthenticationConfigurationBuilder(ProtocolServerConfigurationChildBuilder<?,?,?> builder) {
      this.attributes = SaslAuthenticationConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public SaslAuthenticationConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   public SaslAuthenticationConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   public SaslAuthenticationConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public boolean enabled() {
      return enabled;
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
      if (enabled) {
         sasl.validate();
      }
   }

   @Override
   public SaslAuthenticationConfiguration create() {
      return new SaslAuthenticationConfiguration(attributes.protect(), sasl.create(), enabled);
   }

   @Override
   public Builder<?> read(SaslAuthenticationConfiguration template, Combine combine) {
      this.enabled = template.enabled();
      this.sasl.read(template.sasl(), combine);
      return this;
   }
}
