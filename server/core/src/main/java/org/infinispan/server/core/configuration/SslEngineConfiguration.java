package org.infinispan.server.core.configuration;

import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * SslEngineConfiguration
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public class SslEngineConfiguration extends ConfigurationElement<SslEngineConfiguration> {

   static final AttributeDefinition<String> KEY_STORE_FILE_NAME = AttributeDefinition.builder("keystore-file", null, String.class).build();
   static final AttributeDefinition<String> KEY_STORE_TYPE = AttributeDefinition.builder("keystore-type", null, String.class).build();
   static final AttributeDefinition<char[]> KEY_STORE_PASSWORD = AttributeDefinition.builder("keystore-password", null, char[].class).build();
   static final AttributeDefinition<String> KEY_ALIAS = AttributeDefinition.builder("key-alias", null, String.class).build();
   static final AttributeDefinition<String> PROTOCOL = AttributeDefinition.builder("protocol", null, String.class).build();
   @SuppressWarnings("unchecked")
   static final AttributeDefinition<Supplier<SSLContext>> SSL_CONTEXT = AttributeDefinition.builder("ssl-context", null, (Class<Supplier<SSLContext>>) (Class<?>) Supplier.class).build();
   static final AttributeDefinition<String> TRUST_STORE_FILE_NAME = AttributeDefinition.builder("truststore-file", null, String.class).build();
   static final AttributeDefinition<String> TRUST_STORE_TYPE = AttributeDefinition.builder("truststore-type", null, String.class).build();
   static final AttributeDefinition<char[]> TRUST_STORE_PASSWORD = AttributeDefinition.builder("truststore-password", null, char[].class).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SslEngineConfiguration.class, KEY_STORE_FILE_NAME, KEY_STORE_TYPE, KEY_STORE_PASSWORD,
            KEY_ALIAS, PROTOCOL, SSL_CONTEXT, TRUST_STORE_FILE_NAME, TRUST_STORE_TYPE, TRUST_STORE_PASSWORD);
   }

   SslEngineConfiguration(AttributeSet attributes) {
      super("ssl-engine", attributes);
   }

   public String keyStoreFileName() {
      return attributes.attribute(KEY_STORE_FILE_NAME).get();
   }

   public String keyStoreType() {
      return attributes.attribute(KEY_STORE_TYPE).get();
   }

   public char[] keyStorePassword() {
      return attributes.attribute(KEY_STORE_PASSWORD).get();
   }

   public String keyAlias() {
      return attributes.attribute(KEY_ALIAS).get();
   }

   public SSLContext sslContext() {
      Supplier<SSLContext> supplier = attributes.attribute(SSL_CONTEXT).get();
      return supplier == null ? null : supplier.get();
   }

   Supplier<SSLContext> sslContextSupplier() {
      return attributes.attribute(SSL_CONTEXT).get();
   }

   public String trustStoreFileName() {
      return attributes.attribute(TRUST_STORE_FILE_NAME).get();
   }

   public String trustStoreType() {
      return attributes.attribute(TRUST_STORE_TYPE).get();
   }

   public char[] trustStorePassword() {
      return attributes.attribute(TRUST_STORE_PASSWORD).get();
   }

   public String protocol() {
      return attributes.attribute(PROTOCOL).get();
   }

   public String[] protocols() {
      String p = protocol();
      if (p != null) {
         return new String[]{p};
      } else {
         return null;
      }
   }
}
