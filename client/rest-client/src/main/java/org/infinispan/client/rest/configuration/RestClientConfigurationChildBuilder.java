package org.infinispan.client.rest.configuration;

import java.util.Properties;

/**
 * ConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public interface RestClientConfigurationChildBuilder {

   /**
    * Adds a new remote server
    */
   ServerConfigurationBuilder addServer();

   /**
    * Adds a list of remote servers in the form: host1[:port][;host2[:port]]...
    */
   RestClientConfigurationBuilder addServers(String servers);

   /**
    * Selects the protocol used by the client. See @{@link Protocol}
    */
   RestClientConfigurationBuilder protocol(Protocol protocol);

   /**
    * Configure the client to use <a href="https://http2.github.io/http2-spec/#known-http">Prior Knowledge</a>
    */
   RestClientConfigurationBuilder priorKnowledge(boolean enabled);

   RestClientConfigurationBuilder followRedirects(boolean followRedirects);

   /**
    * This property defines the maximum socket connect timeout before giving up connecting to the server.
    * Defaults to 60000 (1 minute)
    */
   RestClientConfigurationBuilder connectionTimeout(long connectionTimeout);

   /**
    * This property defines the maximum socket read timeout in milliseconds before giving up waiting for bytes from the
    * server. Defaults to 60000 (1 minute)
    */
   RestClientConfigurationBuilder socketTimeout(long socketTimeout);

   /**
    * Security Configuration
    */
   SecurityConfigurationBuilder security();

   /**
    * Affects TCP NODELAY on the TCP stack. Defaults to enabled
    */
   RestClientConfigurationBuilder tcpNoDelay(boolean tcpNoDelay);

   /**
    * Affects TCP KEEPALIVE on the TCP stack. Defaults to disable
    */
   RestClientConfigurationBuilder tcpKeepAlive(boolean keepAlive);

   /**
    * Configures this builder using the specified properties. See {@link RestClientConfigurationBuilder} for a list.
    */
   RestClientConfigurationBuilder withProperties(Properties properties);

   /**
    * Builds a configuration object
    */
   RestClientConfiguration build();

}
