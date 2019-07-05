package org.infinispan.client.rest.configuration;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.TypedProperties;

/**
 * Configuration.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
@BuiltBy(ConfigurationBuilder.class)
public class Configuration {

   private final int connectionTimeout;
   private final List<ServerConfiguration> servers;
   private final int socketTimeout;
   private final SecurityConfiguration security;
   private final boolean tcpNoDelay;
   private final boolean tcpKeepAlive;
   private final Protocol protocol;

   Configuration(List<ServerConfiguration> servers, Protocol protocol, int connectionTimeout, int socketTimeout, SecurityConfiguration security, boolean tcpNoDelay, boolean tcpKeepAlive) {
      this.servers = Collections.unmodifiableList(servers);
      this.protocol = protocol;
      this.connectionTimeout = connectionTimeout;
      this.socketTimeout = socketTimeout;
      this.security = security;
      this.tcpNoDelay = tcpNoDelay;
      this.tcpKeepAlive = tcpKeepAlive;
   }

   public Protocol protocol() {
      return protocol;
   }

   public int connectionTimeout() {
      return connectionTimeout;
   }

   public List<ServerConfiguration> servers() {
      return servers;
   }

   public int socketTimeout() {
      return socketTimeout;
   }

   public SecurityConfiguration security() {
      return security;
   }

   public boolean tcpNoDelay() {
      return tcpNoDelay;
   }

   public boolean tcpKeepAlive() {
      return tcpKeepAlive;
   }

   public Properties properties() {
      TypedProperties properties = new TypedProperties();
      properties.setProperty(ConfigurationProperties.PROTOCOL, protocol().name());
      properties.setProperty(ConfigurationProperties.CONNECT_TIMEOUT, Integer.toString(connectionTimeout()));
      properties.setProperty(ConfigurationProperties.SO_TIMEOUT, socketTimeout());
      properties.setProperty(ConfigurationProperties.TCP_NO_DELAY, tcpNoDelay());
      properties.setProperty(ConfigurationProperties.TCP_KEEP_ALIVE, tcpKeepAlive());

      StringBuilder servers = new StringBuilder();
      for (ServerConfiguration server : servers()) {
         if (servers.length() > 0) {
            servers.append(";");
         }
         servers.append(server.host()).append(":").append(server.port());
      }
      properties.setProperty(ConfigurationProperties.SERVER_LIST, servers.toString());

      properties.setProperty(ConfigurationProperties.USE_SSL, Boolean.toString(security.ssl().enabled()));

      if (security.ssl().keyStoreFileName() != null)
         properties.setProperty(ConfigurationProperties.KEY_STORE_FILE_NAME, security.ssl().keyStoreFileName());

      if (security.ssl().keyStorePassword() != null)
         properties.setProperty(ConfigurationProperties.KEY_STORE_PASSWORD, new String(security.ssl().keyStorePassword()));

      if (security.ssl().keyStoreCertificatePassword() != null)
         properties.setProperty(ConfigurationProperties.KEY_STORE_CERTIFICATE_PASSWORD, new String(security.ssl().keyStoreCertificatePassword()));

      if (security.ssl().trustStoreFileName() != null)
         properties.setProperty(ConfigurationProperties.TRUST_STORE_FILE_NAME, security.ssl().trustStoreFileName());

      if (security.ssl().trustStorePassword() != null)
         properties.setProperty(ConfigurationProperties.TRUST_STORE_PASSWORD, new String(security.ssl().trustStorePassword()));

      if (security.ssl().sniHostName() != null)
         properties.setProperty(ConfigurationProperties.SNI_HOST_NAME, security.ssl().sniHostName());

      if (security.ssl().protocol() != null)
         properties.setProperty(ConfigurationProperties.SSL_PROTOCOL, security.ssl().protocol());

      if (security.ssl().sslContext() != null)
         properties.put(ConfigurationProperties.SSL_CONTEXT, security.ssl().sslContext());

      properties.setProperty(ConfigurationProperties.USE_AUTH, Boolean.toString(security.authentication().enabled()));

      if (security.authentication().mechanism() != null)
         properties.setProperty(ConfigurationProperties.AUTH_MECHANISM, security.authentication().mechanism());

      return properties;
   }
}
