package org.infinispan.cdi.test.testutil;

import org.infinispan.cdi.ConfigureCache;
import org.infinispan.cdi.event.AbstractEventBridge;
import org.infinispan.cdi.event.cache.CacheEventBridge;
import org.infinispan.cdi.event.cachemanager.CacheManagerEventBridge;
import org.infinispan.jcache.annotation.CachePutInterceptor;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.DependencyResolvers;
import org.jboss.shrinkwrap.resolver.api.maven.MavenDependencyResolver;

import java.io.File;

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
      String ideFriendlyPath = "cdi/extension/pom.xml";
      // Figure out an IDE and Maven friendly path:
      String pomPath = new File(ideFriendlyPath).getAbsoluteFile().exists() ? ideFriendlyPath : "pom.xml";

      return ShrinkWrap.create(WebArchive.class, "test.war")
            .addAsWebInfResource(Deployments.class.getResource("/beans.xml"), "beans.xml")
            .addAsLibrary(
                  ShrinkWrap.create(JavaArchive.class, "infinispan-cdi.jar")
                        .addPackage(ConfigureCache.class.getPackage())
                        .addPackage(AbstractEventBridge.class.getPackage())
                        .addPackage(CacheEventBridge.class.getPackage())
                        .addPackage(CacheManagerEventBridge.class.getPackage())
                        .addPackage(CachePutInterceptor.class.getPackage())
                        .addAsManifestResource(ConfigureCache.class.getResource("/META-INF/beans.xml"), "beans.xml")
                        .addAsManifestResource(ConfigureCache.class.getResource("/META-INF/services/javax.enterprise.inject.spi.Extension"), "services/javax.enterprise.inject.spi.Extension")
            )
            .addAsLibraries(
                  DependencyResolvers.use(MavenDependencyResolver.class)
                        .loadMetadataFromPom(pomPath)
                        .artifact("org.jboss.solder:solder-impl")
                        .resolveAs(JavaArchive.class)
            );
   }
}
