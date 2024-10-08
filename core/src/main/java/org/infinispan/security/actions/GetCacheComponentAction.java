package org.infinispan.security.actions;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;

public class GetCacheComponentAction<C> extends AbstractEmbeddedCacheManagerAction<C> {

   private final String cacheName;
   private final Class<C> klass;

   public GetCacheComponentAction(EmbeddedCacheManager cacheManager, String cacheName, Class<C> klass) {
      super(cacheManager);
      this.cacheName = cacheName;
      this.klass = klass;
   }

   @Override
   public C get() {
      ComponentRegistry cr = GlobalComponentRegistry.of(cacheManager).getNamedComponentRegistry(cacheName);
      if (cr == null)
         throw new IllegalLifecycleStateException();

      return cr.getComponent(klass);
   }
}
