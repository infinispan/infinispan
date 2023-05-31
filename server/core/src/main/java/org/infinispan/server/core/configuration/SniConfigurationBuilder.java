package org.infinispan.server.core.configuration;

import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class SniConfigurationBuilder implements Builder<SniConfiguration> {
   private final AttributeSet attributes;
   private final SslConfigurationBuilder ssl;

   SniConfigurationBuilder(SslConfigurationBuilder ssl) {
      this.ssl = ssl;
      this.attributes = SniConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public SniConfigurationBuilder realm(String name) {
      attributes.attribute(SniConfiguration.SECURITY_REALM).set(name);
      return this;
   }

   public SniConfigurationBuilder host(String name) {
      attributes.attribute(SniConfiguration.HOST_NAME).set(name);
      ssl.sniHostName(name);
      return this;
   }

   public SniConfigurationBuilder sslContext(SSLContext sslContext) {
      ssl.sslContext(sslContext);
      return this;
   }

   public SniConfigurationBuilder sslContext(Supplier<SSLContext> sslContext) {
      ssl.sslContext(sslContext);
      return this;
   }

   @Override
   public SniConfiguration create() {
      return new SniConfiguration(attributes.protect());
   }

   @Override
   public SniConfigurationBuilder read(SniConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

}
