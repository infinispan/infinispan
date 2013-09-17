package org.infinispan.configuration.cache;

import org.infinispan.configuration.Builder;
import org.infinispan.configuration.Self;

/**
 * LoaderConfigurationBuilder is an interface which should be implemented by all cache loader builders
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface CacheLoaderConfigurationBuilder<T extends CacheLoaderConfiguration, S extends CacheLoaderConfigurationBuilder<T,S>> extends Builder<T>, LoaderConfigurationChildBuilder<S>, Self<S> {

}
