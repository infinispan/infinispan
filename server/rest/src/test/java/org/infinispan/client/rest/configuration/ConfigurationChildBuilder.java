package org.infinispan.client.rest.configuration;

import java.util.Properties;

/**
 * ConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 10.0
 */
public interface ConfigurationChildBuilder {

   /**
    * Adds a new remote server
    */
   ServerConfigurationBuilder addServer();

   /**
    * Adds a list of remote servers in the form: host1[:port][;host2[:port]]...
    */
   ConfigurationBuilder addServers(String servers);

   /**
    * Selects the protocol used by the client. See @{@link Protocol}
    */
   ConfigurationBuilder protocol(Protocol protocol);

   /**
    * This property defines the maximum socket connect timeout before giving up connecting to the
    * server.
    */
   ConfigurationBuilder connectionTimeout(int connectionTimeout);

   /**
    * This property defines the maximum socket read timeout in milliseconds before giving up waiting
    * for bytes from the server. Defaults to 60000 (1 minute)
    */
   ConfigurationBuilder socketTimeout(int socketTimeout);

   /**
    * Security Configuration
    */
   SecurityConfigurationBuilder security();

   /**
    * Affects TCP NODELAY on the TCP stack. Defaults to enabled
    */
   ConfigurationBuilder tcpNoDelay(boolean tcpNoDelay);

   /**
    * Affects TCP KEEPALIVE on the TCP stack. Defaults to disable
    */
   ConfigurationBuilder tcpKeepAlive(boolean keepAlive);

   /**
    * Configures this builder using the specified properties. See {@link ConfigurationBuilder} for a list.
    */
   ConfigurationBuilder withProperties(Properties properties);

   /**
    * Builds a configuration object
    */
   Configuration build();

}
