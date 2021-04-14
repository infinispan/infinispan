package org.infinispan.server.core.configuration;

import java.util.Map;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * SslConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class SslConfiguration extends ConfigurationElement<SslConfiguration> {

   public static final String DEFAULT_SNI_DOMAIN = "*";

   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).immutable().build();
   static final AttributeDefinition<Boolean> REQUIRE_CLIENT_AUTH = AttributeDefinition.builder("require-client-auth", false).immutable().build();
   private final Map<String, SslEngineConfiguration> sniDomainsConfiguration;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SslConfiguration.class, ENABLED, REQUIRE_CLIENT_AUTH);
   }

   SslConfiguration(AttributeSet attributes, Map<String, SslEngineConfiguration> sniDomainsConfiguration) {
      super("ssl", attributes);
      this.sniDomainsConfiguration = sniDomainsConfiguration;
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public boolean requireClientAuth() {
      return attributes.attribute(REQUIRE_CLIENT_AUTH).get();
   }

   public String keyStoreFileName() {
      return sniDomainsConfiguration.get(DEFAULT_SNI_DOMAIN).keyStoreFileName();
   }

   public char[] keyStorePassword() {
      return sniDomainsConfiguration.get(DEFAULT_SNI_DOMAIN).keyStorePassword();
   }

   public char[] keyStoreCertificatePassword() {
      return sniDomainsConfiguration.get(DEFAULT_SNI_DOMAIN).keyStoreCertificatePassword();
   }

   public SSLContext sslContext() {
      return sniDomainsConfiguration.get(DEFAULT_SNI_DOMAIN).sslContext();
   }

   public String trustStoreFileName() {
      return sniDomainsConfiguration.get(DEFAULT_SNI_DOMAIN).trustStoreFileName();
   }

   public char[] trustStorePassword() {
      return sniDomainsConfiguration.get(DEFAULT_SNI_DOMAIN).trustStorePassword();
   }

   public Map<String, SslEngineConfiguration> sniDomainsConfiguration() {
      return sniDomainsConfiguration;
   }
}
