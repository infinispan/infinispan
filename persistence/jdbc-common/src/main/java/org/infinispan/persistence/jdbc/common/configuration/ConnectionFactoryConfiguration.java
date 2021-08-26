package org.infinispan.persistence.jdbc.common.configuration;

import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;

/**
 * ConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ConnectionFactoryConfiguration {
   Class<? extends ConnectionFactory> connectionFactoryClass();
}
