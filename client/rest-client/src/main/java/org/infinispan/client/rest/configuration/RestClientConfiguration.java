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
@BuiltBy(RestClientConfigurationBuilder.class)
public class RestClientConfiguration {

   private final long connectionTimeout;
   private final List<ServerConfiguration> servers;
   private final long socketTimeout;
   private final SecurityConfiguration security;
   private final boolean tcpNoDelay;
   private final boolean tcpKeepAlive;
   private final Protocol protocol;
   private final String contextPath;
   private final boolean priorKnowledge;
   private final boolean followRedirects;

   RestClientConfiguration(List<ServerConfiguration> servers, Protocol protocol, long connectionTimeout, long socketTimeout, SecurityConfiguration security, boolean tcpNoDelay, boolean tcpKeepAlive, String contextPath, boolean priorKnowledge, boolean followRedirects) {
      this.servers = Collections.unmodifiableList(servers);
      this.protocol = protocol;
      this.connectionTimeout = connectionTimeout;
      this.socketTimeout = socketTimeout;
      this.security = security;
      this.tcpNoDelay = tcpNoDelay;
      this.tcpKeepAlive = tcpKeepAlive;
      this.contextPath = contextPath;
      this.priorKnowledge = priorKnowledge;
      this.followRedirects = followRedirects;
   }

   public Protocol protocol() {
      return protocol;
   }

   public boolean priorKnowledge() {
      return priorKnowledge;
   }

   public boolean followRedirects() {
      return followRedirects;
   }

   public long connectionTimeout() {
      return connectionTimeout;
   }

   public List<ServerConfiguration> servers() {
      return servers;
   }

   public long socketTimeout() {
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

   public String contextPath() {
      return contextPath;
   }

   public Properties properties() {
      TypedProperties properties = new TypedProperties();
      properties.setProperty(RestClientConfigurationProperties.PROTOCOL, protocol().name());
      properties.setProperty(RestClientConfigurationProperties.CONNECT_TIMEOUT, Long.toString(connectionTimeout()));
      properties.setProperty(RestClientConfigurationProperties.SO_TIMEOUT, socketTimeout());
      properties.setProperty(RestClientConfigurationProperties.TCP_NO_DELAY, tcpNoDelay());
      properties.setProperty(RestClientConfigurationProperties.TCP_KEEP_ALIVE, tcpKeepAlive());
      properties.setProperty(RestClientConfigurationProperties.CONTEXT_PATH, tcpKeepAlive());

      StringBuilder servers = new StringBuilder();
      for (ServerConfiguration server : servers()) {
         if (servers.length() > 0) {
            servers.append(";");
         }
         servers.append(server.host()).append(":").append(server.port());
      }
      properties.setProperty(RestClientConfigurationProperties.SERVER_LIST, servers.toString());

      properties.setProperty(RestClientConfigurationProperties.USE_SSL, Boolean.toString(security.ssl().enabled()));

      if (security.ssl().keyStoreFileName() != null)
         properties.setProperty(RestClientConfigurationProperties.KEY_STORE_FILE_NAME, security.ssl().keyStoreFileName());

      if (security.ssl().keyStorePassword() != null)
         properties.setProperty(RestClientConfigurationProperties.KEY_STORE_PASSWORD, new String(security.ssl().keyStorePassword()));

      if (security.ssl().keyStoreCertificatePassword() != null)
         properties.setProperty(RestClientConfigurationProperties.KEY_STORE_CERTIFICATE_PASSWORD, new String(security.ssl().keyStoreCertificatePassword()));

      if (security.ssl().trustStoreFileName() != null)
         properties.setProperty(RestClientConfigurationProperties.TRUST_STORE_FILE_NAME, security.ssl().trustStoreFileName());

      if (security.ssl().trustStorePassword() != null)
         properties.setProperty(RestClientConfigurationProperties.TRUST_STORE_PASSWORD, new String(security.ssl().trustStorePassword()));

      if (security.ssl().sniHostName() != null)
         properties.setProperty(RestClientConfigurationProperties.SNI_HOST_NAME, security.ssl().sniHostName());

      if (security.ssl().protocol() != null)
         properties.setProperty(RestClientConfigurationProperties.SSL_PROTOCOL, security.ssl().protocol());

      if (security.ssl().sslContext() != null)
         properties.put(RestClientConfigurationProperties.SSL_CONTEXT, security.ssl().sslContext());

      if (security.ssl().trustManagers() != null)
         properties.put(RestClientConfigurationProperties.TRUST_MANAGERS, security.ssl().trustManagers());

      properties.setProperty(RestClientConfigurationProperties.USE_AUTH, Boolean.toString(security.authentication().enabled()));

      if (security.authentication().mechanism() != null)
         properties.setProperty(RestClientConfigurationProperties.AUTH_MECHANISM, security.authentication().mechanism());

      return properties;
   }

   public String toURI() {
      StringBuilder sb = new StringBuilder("http");
      if (security.ssl().enabled()) {
         sb.append('s');
      }
      sb.append("://");
      if (security.authentication().enabled()) {
         sb.append(security.authentication().username()).append(':').append(security.authentication().password()).append('@');
      }
      for (int i = 0; i < servers.size(); i++) {
         if (i > 0) {
            sb.append(";");
         }
         ServerConfiguration server = servers.get(i);
         sb.append(server.host()).append(":").append(server.port());
      }

      return sb.toString();
   }
}
