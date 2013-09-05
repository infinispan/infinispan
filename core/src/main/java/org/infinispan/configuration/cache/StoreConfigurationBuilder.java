package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Self;

import java.util.Properties;

/**
 * LoaderConfigurationBuilder is an interface which should be implemented by all cache loader builders
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface StoreConfigurationBuilder<T extends StoreConfiguration, S extends StoreConfigurationBuilder<T,S>> extends Builder<T>, StoreConfigurationChildBuilder<S>, Self<S> {
}
