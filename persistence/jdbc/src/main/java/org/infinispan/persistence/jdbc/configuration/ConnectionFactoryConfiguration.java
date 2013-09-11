package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;

/**
 * ConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ConnectionFactoryConfiguration {
   Class<? extends ConnectionFactory> connectionFactoryClass();
}
