package org.infinispan.server.memcached.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.configuration.AuthenticationConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationChildBuilder;
import org.infinispan.server.core.configuration.SaslConfigurationBuilder;

/**
 * MemcachedAuthenticationConfigurationBuilder.
 *
 * @since 7.0
 */
public class MemcachedAuthenticationConfigurationBuilder implements AuthenticationConfigurationBuilder<MemcachedAuthenticationConfiguration> {
   private final AttributeSet attributes;
   private final SaslConfigurationBuilder sasl = new SaslConfigurationBuilder();
   private final TextAuthenticationConfigurationBuilder text = new TextAuthenticationConfigurationBuilder();

   public MemcachedAuthenticationConfigurationBuilder(ProtocolServerConfigurationChildBuilder<?,?,?> builder) {
      this.attributes = MemcachedAuthenticationConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public MemcachedAuthenticationConfigurationBuilder enable() {
      return enabled(true);
   }

   public MemcachedAuthenticationConfigurationBuilder disable() {
      return enabled(false);
   }

   public MemcachedAuthenticationConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(MemcachedAuthenticationConfiguration.ENABLED).set(enabled);
      return this;
   }

   public boolean enabled() {
      return attributes.attribute(MemcachedAuthenticationConfiguration.ENABLED).get();
   }

   public MemcachedAuthenticationConfigurationBuilder securityRealm(String name) {
      attributes.attribute(MemcachedAuthenticationConfiguration.SECURITY_REALM).set(name);
      return this;
   }

   public String securityRealm() {
      return attributes.attribute(MemcachedAuthenticationConfiguration.SECURITY_REALM).get();
   }

   public boolean hasSecurityRealm() {
      return !attributes.attribute(MemcachedAuthenticationConfiguration.SECURITY_REALM).isNull();
   }

   public SaslConfigurationBuilder sasl() {
      return sasl;
   }

   public TextAuthenticationConfigurationBuilder text() {
      return text;
   }

   @Override
   public void validate() {
      if (enabled()) {
         sasl.validate();
         text.validate();
      }
   }

   @Override
   public MemcachedAuthenticationConfiguration create() {
      return new MemcachedAuthenticationConfiguration(attributes.protect(), sasl.create(), text.create());
   }

   @Override
   public Builder<?> read(MemcachedAuthenticationConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      this.sasl.read(template.sasl(), combine);
      this.text.read(template.text(), combine);
      return this;
   }
}
