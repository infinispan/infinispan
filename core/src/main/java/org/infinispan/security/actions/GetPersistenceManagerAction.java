package org.infinispan.security.actions;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;

public class GetPersistenceManagerAction extends AbstractEmbeddedCacheManagerAction<PersistenceManager> {

   private final String cacheName;

   public GetPersistenceManagerAction(EmbeddedCacheManager cacheManager, String cacheName) {
      super(cacheManager);
      this.cacheName = cacheName;
   }

   @Override
   public PersistenceManager get() {
      ComponentRegistry cr = cacheManager.getGlobalComponentRegistry().getNamedComponentRegistry(cacheName);
      if (cr == null)
         throw new IllegalLifecycleStateException();

      return cr.getComponent(PersistenceManager.class);
   }
}
