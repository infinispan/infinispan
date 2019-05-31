package org.infinispan.jcache.embedded;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;

@BuiltBy(JStoreAdapterConfigurationBuilder.class)
@ConfigurationFor(JCacheLoaderAdapter.class)
public class JStoreAdapterConfiguration extends AbstractStoreConfiguration {

   public JStoreAdapterConfiguration(AttributeSet attributes, AsyncStoreConfiguration async) {
      super(attributes, async);
   }
}
