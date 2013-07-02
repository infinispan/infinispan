package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.commons.util.TypedProperties;

/**
 * AbstractLoaderConfiguration. Base class for loader configuration
 *
 * @author Pete Muir
 * @author Tristan Tarrant
 *
 * @since 5.1
 */
public abstract class AbstractLoaderConfiguration extends AbstractTypedPropertiesConfiguration implements CacheLoaderConfiguration {

   protected AbstractLoaderConfiguration(TypedProperties properties) {
      super(properties);
   }
}
