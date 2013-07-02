package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;

/**
 * ConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ConnectionFactoryConfiguration {
   Class<? extends ConnectionFactory> connectionFactoryClass();
}
