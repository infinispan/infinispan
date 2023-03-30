package org.infinispan.server.core.configuration;


import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @author Tristan Tarrant
 * @since 5.3
 */
public class MockServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<MockServerConfiguration, MockServerConfigurationBuilder, MockAuthenticationConfiguration> {

   public MockServerConfigurationBuilder() {
      super(12345, MockServerConfiguration.attributeDefinitionSet());
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public MockServerConfigurationBuilder self() {
      return this;
   }

   @Override
   public AuthenticationConfigurationBuilder<MockAuthenticationConfiguration> authentication() {
      return new MockAuthenticationConfigurationBuilder();
   }

   @Override
   public MockServerConfiguration build() {
      return build(true);
   }

   public MockServerConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public MockServerConfiguration create() {
      return new MockServerConfiguration(attributes.protect(), authentication().create(), ssl.create(), ipFilter.create());
   }
}
