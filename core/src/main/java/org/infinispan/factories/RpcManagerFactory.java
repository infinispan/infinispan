package org.infinispan.factories;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;

/**
 * A factory for the RpcManager
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@DefaultFactoryFor(classes = RpcManager.class)
public class RpcManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      if (!configuration.clustering().cacheMode().isClustered())
         return null;

      // only do this if we have a transport configured!
      if (!globalConfiguration.isClustered())
         throw new CacheConfigurationException("Transport should be configured in order to use clustered caches");

      return componentType.cast(new RpcManagerImpl());
   }

}
