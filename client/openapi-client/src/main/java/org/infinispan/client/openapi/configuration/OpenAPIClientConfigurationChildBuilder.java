package org.infinispan.client.openapi.configuration;

import java.util.Properties;

/**
 * ConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 16.0
 */
public interface OpenAPIClientConfigurationChildBuilder {

   /**
    * Adds a new remote server
    */
   ServerConfigurationBuilder addServer();

   /**
    * Adds a list of remote servers in the form: host1[:port][;host2[:port]]...
    */
   OpenAPIClientConfigurationBuilder addServers(String servers);

   /**
    * Selects the protocol used by the client. See @{@link Protocol}
    */
   OpenAPIClientConfigurationBuilder protocol(Protocol protocol);

   /**
    * Configure the client to use <a href="https://http2.github.io/http2-spec/#known-http">Prior Knowledge</a>
    */
   OpenAPIClientConfigurationBuilder priorKnowledge(boolean enabled);

   OpenAPIClientConfigurationBuilder followRedirects(boolean followRedirects);

   /**
    * This property defines the maximum socket connect timeout before giving up connecting to the server.
    * Defaults to 60000 (1 minute)
    */
   OpenAPIClientConfigurationBuilder connectionTimeout(long connectionTimeout);

   /**
    * This property defines the maximum socket read timeout in milliseconds before giving up waiting for bytes from the
    * server. Defaults to 60000 (1 minute)
    */
   OpenAPIClientConfigurationBuilder socketTimeout(long socketTimeout);

   OpenAPIClientConfigurationBuilder pingOnCreate(boolean pingOnCreate);

   /**
    * Security Configuration
    */
   SecurityConfigurationBuilder security();

   /**
    * Affects TCP NODELAY on the TCP stack. Defaults to enabled
    */
   OpenAPIClientConfigurationBuilder tcpNoDelay(boolean tcpNoDelay);

   /**
    * Affects TCP KEEPALIVE on the TCP stack. Defaults to disable
    */
   OpenAPIClientConfigurationBuilder tcpKeepAlive(boolean keepAlive);

   /**
    * Configures this builder using the specified properties. See {@link OpenAPIClientConfigurationBuilder} for a list.
    */
   OpenAPIClientConfigurationBuilder withProperties(Properties properties);

   /**
    * Builds a configuration object
    */
   OpenAPIClientConfiguration build();

}
