package org.infinispan.cdi.embedded.test.testutil;

import org.infinispan.cdi.embedded.ConfigureCache;
import org.infinispan.cdi.embedded.event.AbstractEventBridge;
import org.infinispan.cdi.embedded.event.cache.CacheEventBridge;
import org.infinispan.cdi.embedded.event.cachemanager.CacheManagerEventBridge;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;

/**
 * Arquillian deployment utility class.
 *
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
public final class Deployments {
   /**
    * The base deployment web archive. The CDI extension is packaged as an individual jar.
    */
   public static WebArchive baseDeployment() {
      return ShrinkWrap.create(WebArchive.class, "test.war")
            .addAsWebInfResource(Deployments.class.getResource("/META-INF/beans.xml"), "beans.xml")
            .addAsLibrary(
                  ShrinkWrap.create(JavaArchive.class, "infinispan-cdi-embedded.jar")
                        .addPackage(ConfigureCache.class.getPackage())
                        .addPackage(AbstractEventBridge.class.getPackage())
                        .addPackage(CacheEventBridge.class.getPackage())
                        .addPackage(CacheManagerEventBridge.class.getPackage())
                        .addAsManifestResource(ConfigureCache.class.getResource("/META-INF/beans.xml"), "beans.xml")
                        .addAsManifestResource(ConfigureCache.class.getResource("/META-INF/services/jakarta.enterprise.inject.spi.Extension"), "services/jakarta.enterprise.inject.spi.Extension")
            );
   }
}
