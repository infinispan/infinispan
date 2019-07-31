package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.Element.ENCRYPTION;

import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * SslConfiguration.
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class SslConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false, Boolean.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<SSLContext> SSL_CONTEXT = AttributeDefinition.builder("sslContext", null, SSLContext.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<String> SNI_HOSTNAME = AttributeDefinition.builder("sniHostname", null, String.class).immutable().build();
   static final AttributeDefinition<String> PROTOCOL = AttributeDefinition.builder("protocol", null, String.class).immutable().build();

   static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(ENCRYPTION.getLocalName());

   private final AttributeSet attributes;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SslConfiguration.class, ENABLED, SNI_HOSTNAME, PROTOCOL);
   }

   private KeyStoreConfiguration keyStoreConfiguration;
   private TrustStoreConfiguration trustStoreConfiguration;
   private List<ConfigurationInfo> subElements;

   SslConfiguration(AttributeSet attributes, KeyStoreConfiguration keyStoreConfiguration, TrustStoreConfiguration trustStoreConfiguration) {
      this.attributes = attributes.checkProtection();
      this.keyStoreConfiguration = keyStoreConfiguration;
      this.trustStoreConfiguration = trustStoreConfiguration;
      this.subElements = Arrays.asList(keyStoreConfiguration, trustStoreConfiguration);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   public KeyStoreConfiguration keyStoreConfiguration() {
      return keyStoreConfiguration;
   }

   public TrustStoreConfiguration trustStoreConfiguration() {
      return trustStoreConfiguration;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public AttributeSet attributes() {
      return attributes;
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

   public String sniHostName() {
      return attributes.attribute(SNI_HOSTNAME).get();
   }

   public String protocol() {
      return attributes.attribute(PROTOCOL).get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SslConfiguration that = (SslConfiguration) o;

      if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) return false;
      if (keyStoreConfiguration != null ? !keyStoreConfiguration.equals(that.keyStoreConfiguration) : that.keyStoreConfiguration != null)
         return false;
      return trustStoreConfiguration != null ? trustStoreConfiguration.equals(that.trustStoreConfiguration) : that.trustStoreConfiguration == null;
   }

   @Override
   public int hashCode() {
      int result = attributes != null ? attributes.hashCode() : 0;
      result = 31 * result + (keyStoreConfiguration != null ? keyStoreConfiguration.hashCode() : 0);
      result = 31 * result + (trustStoreConfiguration != null ? trustStoreConfiguration.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "SslConfiguration{" +
            "attributes=" + attributes +
            ", keyStoreConfiguration=" + keyStoreConfiguration +
            ", trustStoreConfiguration=" + trustStoreConfiguration +
            '}';
   }

}
