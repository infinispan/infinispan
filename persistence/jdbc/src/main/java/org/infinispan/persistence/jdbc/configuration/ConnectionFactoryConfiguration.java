package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;

/**
 * ConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ConnectionFactoryConfiguration extends ConfigurationInfo {
   Class<? extends ConnectionFactory> connectionFactoryClass();
}
