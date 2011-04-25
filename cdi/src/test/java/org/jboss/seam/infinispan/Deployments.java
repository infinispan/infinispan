package org.jboss.seam.infinispan;

import org.jboss.seam.infinispan.event.cache.CacheEventBridge;
import org.jboss.seam.infinispan.event.cachemanager.CacheManagerEventBridge;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

public class Deployments {

   public static JavaArchive baseDeployment() {
      return ShrinkWrap.create(JavaArchive.class, "test.jar")
            .addPackage(Infinispan.class.getPackage())
            .addPackage(CacheEventBridge.class.getPackage())
            .addPackage(CacheManagerEventBridge.class.getPackage());
   }

}
