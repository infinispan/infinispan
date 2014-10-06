package org.infinispan.cdi.test.testutil;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.cdi.event.AbstractEventBridge;
import org.infinispan.cdi.event.cache.CacheEventBridge;
import org.infinispan.cdi.event.cachemanager.CacheManagerEventBridge;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;

/**
 * Arquillian deployment utility class.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public final class Deployments {
   /**
    * The base deployment web archive. The CDI extension is packaged as an individual jar.
    */
   public static WebArchive baseDeployment() {
      return ShrinkWrap.create(WebArchive.class, "test.war")
            .addAsWebInfResource(Deployments.class.getResource("/beans.xml"), "beans.xml")
            .addAsLibrary(
                  ShrinkWrap.create(JavaArchive.class, "infinispan-cdi.jar")
                        .addPackage(ConfigureCache.class.getPackage())
                        .addPackage(AbstractEventBridge.class.getPackage())
                        .addPackage(CacheEventBridge.class.getPackage())
                        .addPackage(CacheManagerEventBridge.class.getPackage())
                        .addAsManifestResource(ConfigureCache.class.getResource("/META-INF/beans.xml"), "beans.xml")
                        .addAsManifestResource(ConfigureCache.class.getResource("/META-INF/services/javax.enterprise.inject.spi.Extension"), "services/javax.enterprise.inject.spi.Extension")
            );
   }
}
