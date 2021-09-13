package org.infinispan.server.core.configuration;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class EncryptionConfigurationBuilder implements Builder<EncryptionConfiguration> {
   private final AttributeSet attributes;

   private final List<SniConfigurationBuilder> sniConfigurations = new ArrayList<>();
   private final SslConfigurationBuilder ssl;

   public EncryptionConfigurationBuilder(SslConfigurationBuilder sslConfigurationBuilder) {
      this.ssl = sslConfigurationBuilder;
      this.attributes = EncryptionConfiguration.attributeDefinitionSet();
   }

   public SniConfigurationBuilder addSni() {
      SniConfigurationBuilder sni = new SniConfigurationBuilder(ssl);
      sniConfigurations.add(sni);
      return sni;
   }

   public EncryptionConfigurationBuilder sslContext(SSLContext context) {
      ssl.sslContext(context);
      return this;
   }

   public EncryptionConfigurationBuilder sslContext(Supplier<SSLContext> context) {
      ssl.sslContext(context);
      return this;
   }

   public EncryptionConfigurationBuilder realm(String name) {
      ssl.enable();
      attributes.attribute(EncryptionConfiguration.SECURITY_REALM).set(name);
      return this;
   }

   public EncryptionConfigurationBuilder requireClientAuth(boolean require) {
      attributes.attribute(EncryptionConfiguration.REQUIRE_CLIENT_AUTH).set(require);
      ssl.requireClientAuth(require);
      return this;
   }

   @Override
   public EncryptionConfiguration create() {
      List<SniConfiguration> snis = sniConfigurations.stream().map(SniConfigurationBuilder::create).collect(toList());
      return new EncryptionConfiguration(attributes.protect(), snis);
   }

   @Override
   public EncryptionConfigurationBuilder read(EncryptionConfiguration template) {
      attributes.read(template.attributes());
      sniConfigurations.clear();
      template.sniConfigurations().forEach(s -> addSni().read(s));
      return this;
   }
}
