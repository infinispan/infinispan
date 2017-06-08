package org.infinispan.persistence.remote.configuration;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Util;

/**
 * SslConfiguration.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class SslConfiguration {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false, Boolean.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> KEYSTORE_FILENAME = AttributeDefinition.builder("keyStoreFilename", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> KEYSTORE_TYPE = AttributeDefinition.builder("keyStoreType", "JKS", String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> KEYSTORE_PASSWORD = AttributeDefinition.builder("keyStorePassword", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> KEYSTORE_CERTIFICATE_PASSWORD = AttributeDefinition.builder("keyStoreCertificatePassword", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> KEY_ALIAS = AttributeDefinition.builder("keyAlias", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<SSLContext> SSL_CONTEXT = AttributeDefinition.builder("sslContext", null, SSLContext.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> TRUSTSTORE_FILENAME = AttributeDefinition.builder("trustStoreFilename", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> TRUSTSTORE_TYPE = AttributeDefinition.builder("trustStoreType", "JKS", String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> TRUSTSTORE_PASSWORD = AttributeDefinition.builder("trustStorePassword", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> SNI_HOSTNAME = AttributeDefinition.builder("sniHostname", null, String.class).immutable().build();
   static final AttributeDefinition<String> PROTOCOL = AttributeDefinition.builder("protocol", null, String.class).immutable().build();

   private final AttributeSet attributes;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SslConfiguration.class, ENABLED, KEYSTORE_FILENAME, KEYSTORE_TYPE, KEYSTORE_PASSWORD, KEYSTORE_CERTIFICATE_PASSWORD,
            KEY_ALIAS, TRUSTSTORE_FILENAME, TRUSTSTORE_TYPE, TRUSTSTORE_PASSWORD, SNI_HOSTNAME, PROTOCOL);
   }

   SslConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   AttributeSet attributes() {
      return attributes;
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
      return Util.toCharArray(attributes.attribute(KEYSTORE_PASSWORD).get());
   }

   public char[] keyStoreCertificatePassword() {
      return Util.toCharArray(attributes.attribute(KEYSTORE_CERTIFICATE_PASSWORD).get());
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
      return Util.toCharArray(attributes.attribute(TRUSTSTORE_PASSWORD).get());
   }

   public String sniHostName() {
      return attributes.attribute(SNI_HOSTNAME).get();
   }

   public String protocol() {
      return attributes.attribute(PROTOCOL).get();
   }

   @Override
   public String toString() {
      return attributes.toString();
   }

   @Override
   public boolean equals(Object o) {
      SslConfiguration other = (SslConfiguration) o;
      return attributes.equals(other.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }
}
