package org.infinispan.jcache.embedded;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;

@BuiltBy(JCacheWriterAdapterConfigurationBuilder.class)
@ConfigurationFor(JCacheWriterAdapter.class)
public class JCacheWriterAdapterConfiguration extends AbstractStoreConfiguration {

   public JCacheWriterAdapterConfiguration(AttributeSet attributes, AsyncStoreConfiguration async) {
      super(attributes, async);
   }
}
