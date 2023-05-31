package org.infinispan.server.memcached.configuration;

import static org.infinispan.server.memcached.configuration.TextAuthenticationConfiguration.AUTHENTICATOR;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.security.UsernamePasswordAuthenticator;

/**
 * @since 15.0
 **/
public class TextAuthenticationConfigurationBuilder implements Builder<TextAuthenticationConfiguration> {
   private final AttributeSet attributes = TextAuthenticationConfiguration.attributeDefinitionSet();

   public TextAuthenticationConfigurationBuilder authenticator(UsernamePasswordAuthenticator authenticator) {
      attributes.attribute(AUTHENTICATOR).set(authenticator);
      return this;
   }

   @Override
   public TextAuthenticationConfiguration create() {
      return new TextAuthenticationConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(TextAuthenticationConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }
}
