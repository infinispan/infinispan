package org.infinispan.persistence.remote.configuration;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * SslConfiguration.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class SslConfiguration extends ConfigurationElement<SslConfiguration> {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false, Boolean.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<SSLContext> SSL_CONTEXT = AttributeDefinition.builder("sslContext", null, SSLContext.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> SNI_HOSTNAME = AttributeDefinition.builder("sniHostname", null, String.class).immutable().build();
   static final AttributeDefinition<Boolean> HOSTNAME_VALIDATION = AttributeDefinition.builder("ssl-hostname-validation", true).immutable().build();
   static final AttributeDefinition<String> PROTOCOL = AttributeDefinition.builder("protocol", null, String.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SslConfiguration.class, ENABLED, SNI_HOSTNAME,HOSTNAME_VALIDATION, PROTOCOL);
   }

   private final KeyStoreConfiguration keyStoreConfiguration;
   private final TrustStoreConfiguration trustStoreConfiguration;

   SslConfiguration(AttributeSet attributes, KeyStoreConfiguration keyStoreConfiguration, TrustStoreConfiguration trustStoreConfiguration) {
      super(Element.ENCRYPTION, attributes, keyStoreConfiguration, trustStoreConfiguration);
      this.keyStoreConfiguration = keyStoreConfiguration;
      this.trustStoreConfiguration = trustStoreConfiguration;
   }

   public KeyStoreConfiguration keyStoreConfiguration() {
      return keyStoreConfiguration;
   }

   public TrustStoreConfiguration trustStoreConfiguration() {
      return trustStoreConfiguration;
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   public String keyStoreFileName() {
      return keyStoreConfiguration.keyStoreFileName();
   }

   public String keyStoreType() {
      return keyStoreConfiguration.keyStoreType();
   }

   public char[] keyStorePassword() {
      return keyStoreConfiguration.keyStorePassword();
   }

   public char[] keyStoreCertificatePassword() {
      return keyStoreConfiguration.keyStoreCertificatePassword();
   }

   public String keyAlias() {
      return keyStoreConfiguration.keyAlias();
   }

   public SSLContext sslContext() {
      return attributes.attribute(SSL_CONTEXT).get();
   }

   public String trustStoreFileName() {
      return trustStoreConfiguration.trustStoreFileName();
   }

   public String trustStoreType() {
      return trustStoreConfiguration.trustStoreType();
   }

   public char[] trustStorePassword() {
      return trustStoreConfiguration.trustStorePassword();
   }

   public boolean hostnameValidation() {
      return attributes.attribute(HOSTNAME_VALIDATION).get();
   }

   public String sniHostName() {
      return attributes.attribute(SNI_HOSTNAME).get();
   }

   public String protocol() {
      return attributes.attribute(PROTOCOL).get();
   }
}
