package org.infinispan.jcache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractLoaderConfiguration;

@BuiltBy(JCacheLoaderAdapterConfigurationBuilder.class)
@ConfigurationFor(JCacheLoaderAdapter.class)
public class JCacheLoaderAdapterConfiguration extends AbstractLoaderConfiguration {

   protected JCacheLoaderAdapterConfiguration(TypedProperties properties) {
      super(properties);
   }

}
