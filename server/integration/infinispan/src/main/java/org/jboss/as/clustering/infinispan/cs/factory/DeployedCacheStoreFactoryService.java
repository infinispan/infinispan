package org.jboss.as.clustering.infinispan.cs.factory;

import org.infinispan.persistence.factory.CacheStoreFactory;
import org.jboss.as.clustering.infinispan.InfinispanLogger;
import org.jboss.msc.service.*;

/**
 * Service wrapper for {@link org.jboss.as.clustering.infinispan.cs.factory.DeployedCacheStoreFactory}.
 *
 * @author Sebastian Laskawiec
 */
public class DeployedCacheStoreFactoryService implements Service<CacheStoreFactory> {

   public static final ServiceName SERVICE_NAME = ServiceName.JBOSS.append("DeployedCacheStoreFactoryService");

   private final DeployedCacheStoreFactory internalImplementation = new DeployedCacheStoreFactory();

   @Override
   public void start(StartContext context) throws StartException {
      InfinispanLogger.ROOT_LOGGER.debugf("Starting DeployedCacheStoreFactoryService " + internalImplementation);
   }

   @Override
   public void stop(StopContext context) {
      InfinispanLogger.ROOT_LOGGER.debugf("Stopping DeployedCacheStoreFactoryService " + internalImplementation);
   }

   @Override
   public DeployedCacheStoreFactory getValue() throws IllegalStateException, IllegalArgumentException {
      return internalImplementation;
   }
}
