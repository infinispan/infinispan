package org.infinispan.hotrod.configuration;

import java.security.KeyStore;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * SslConfiguration.
 *
 * @since 14.0
 */
public class SslConfiguration extends ConfigurationElement<SslConfiguration> {
   static final AttributeDefinition<String[]> CIPHERS = AttributeDefinition.builder("ciphers", null, String[].class).immutable().build();
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("use-ssl", false, Boolean.class).immutable().build();
   static final AttributeDefinition<String> KEY_ALIAS = AttributeDefinition.builder("key-alias", null, String.class).immutable().build();
   static final AttributeDefinition<String> KEYSTORE_FILENAME = AttributeDefinition.builder("keystore-filename", null, String.class).immutable().build();
   static final AttributeDefinition<char[]> KEYSTORE_PASSWORD = AttributeDefinition.builder("keystore-password", null, char[].class).immutable().build();
   static final AttributeDefinition<String> KEYSTORE_TYPE = AttributeDefinition.builder("keystore-type", KeyStore.getDefaultType(), String.class).immutable().build();
   static final AttributeDefinition<String> PROTOCOL = AttributeDefinition.builder("protocol", null, String.class).immutable().build();
   static final AttributeDefinition<String> PROVIDER = AttributeDefinition.builder("provider", null, String.class).immutable().build();
   static final AttributeDefinition<String> SNI_HOSTNAME = AttributeDefinition.builder("sni-hostname", null, String.class).immutable().build();
   static final AttributeDefinition<SSLContext> SSL_CONTEXT = AttributeDefinition.builder("ssl-context", null, SSLContext.class).immutable().build();
   static final AttributeDefinition<String> TRUSTSTORE_FILENAME = AttributeDefinition.builder("truststore-filename", null, String.class).immutable().build();
   static final AttributeDefinition<char[]> TRUSTSTORE_PASSWORD = AttributeDefinition.builder("truststore-password", null, char[].class).immutable().build();
   static final AttributeDefinition<String> TRUSTSTORE_TYPE = AttributeDefinition.builder("truststore-type", KeyStore.getDefaultType(), String.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SslConfiguration.class,
            ENABLED, KEY_ALIAS, KEYSTORE_FILENAME, KEYSTORE_PASSWORD, KEYSTORE_TYPE, PROTOCOL,
            PROVIDER, SNI_HOSTNAME, SSL_CONTEXT, TRUSTSTORE_FILENAME, TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE);
   }

   SslConfiguration(AttributeSet attributes) {
      super("ssl", attributes);
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public String keyStoreFileName() {
      return attributes.attribute(KEYSTORE_FILENAME).get();
   }

   public String keyStoreType() {
      return attributes.attribute(KEYSTORE_TYPE).get();
   }

   public char[] keyStorePassword() {
      return attributes.attribute(KEYSTORE_PASSWORD).get();
   }

   public String keyAlias() {
      return attributes.attribute(KEY_ALIAS).get();
   }

   public SSLContext sslContext() {
      return attributes.attribute(SSL_CONTEXT).get();
   }

   public String trustStoreFileName() {
      return attributes.attribute(TRUSTSTORE_FILENAME).get();
   }

   public String trustStoreType() {
      return attributes.attribute(TRUSTSTORE_TYPE).get();
   }

   public char[] trustStorePassword() {
      return attributes.attribute(TRUSTSTORE_PASSWORD).get();
   }

   public String sniHostName() {
      return attributes.attribute(SNI_HOSTNAME).get();
   }

   public String protocol() {
      return attributes.attribute(PROTOCOL).get();
   }

   public String[] ciphers() {
      return attributes.attribute(CIPHERS).get();
   }

   public String provider() {
      return attributes.attribute(PROVIDER).get();
   }
}
