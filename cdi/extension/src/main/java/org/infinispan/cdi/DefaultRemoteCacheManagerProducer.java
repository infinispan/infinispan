package org.infinispan.cdi;

import org.infinispan.cdi.util.defaultbean.DefaultBean;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

/**
 * <p>The default {@link RemoteCacheManager} producer.</p>
 *
 * <p>The remote cache manager used by default can be overridden by creating a producer which produces the new default
 * remote cache manager. The remote cache manager produced must have the scope {@link ApplicationScoped} and the
 * {@linkplain javax.enterprise.inject.Default Default} qualifier.</p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class DefaultRemoteCacheManagerProducer {
   /**
    * Produces the default remote cache manager with the default settings.
    *
    * @return the default remote cache manager.
    * @see org.infinispan.client.hotrod.RemoteCacheManager#RemoteCacheManager()
    */
   @Produces
   @ApplicationScoped
   @DefaultBean(RemoteCacheManager.class)
   public RemoteCacheManager getDefaultRemoteCacheManager() {
      return new RemoteCacheManager();
   }

   /**
    * Stops the default remote cache manager when the corresponding instance is released.
    *
    * @param defaultRemoteCacheManager the default remote cache manager.
    */
   private void stopRemoteCacheManager(@Disposes RemoteCacheManager defaultRemoteCacheManager) {
      defaultRemoteCacheManager.stop();
   }

}
