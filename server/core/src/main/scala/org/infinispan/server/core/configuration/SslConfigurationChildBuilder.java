package org.infinispan.server.core.configuration;

import org.infinispan.commons.configuration.Builder;

/**
 * ProtocolServerConfigurationChildBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public interface SslConfigurationChildBuilder extends Builder<SslEngineConfiguration> {

   SslEngineConfigurationBuilder sniHostName(String domain);

}
