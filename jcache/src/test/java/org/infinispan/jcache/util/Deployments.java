package org.infinispan.jcache.util;

import org.infinispan.jcache.JCache;
import org.infinispan.jcache.annotation.InjectedCacheResolver;
import org.infinispan.jcache.interceptor.ExpirationTrackingInterceptor;
import org.infinispan.jcache.logging.Log;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.DependencyResolvers;
import org.jboss.shrinkwrap.resolver.api.maven.MavenDependencyResolver;

/**
 * Arquillian deployment utility class.
 *
 */
public final class Deployments {
   /**
    * The base deployment web archive. infinispan-jcache is packaged as an individual jar.
    */
   public static WebArchive baseDeploymentInjectedInterceptors() {

      final MavenDependencyResolver resolver = DependencyResolvers.use(
            MavenDependencyResolver.class).loadMetadataFromPom("pom.xml");

      return ShrinkWrap.create(WebArchive.class, "test.war")
            .addAsWebInfResource(Deployments.class.getResource("/beans-injectedinterceptors.xml"), "beans.xml")
            .addAsLibraries(resolver.artifact("org.infinispan:infinispan-cdi").resolveAsFiles())
            .addAsLibrary(
                  ShrinkWrap.create(JavaArchive.class, "infinispan-jcache.jar")
                  .addPackage(JCache.class.getPackage())
                  .addPackage(InjectedCacheResolver.class.getPackage())
                  .addPackage(ExpirationTrackingInterceptor.class.getPackage())
                  .addPackage(Log.class.getPackage())
                  .addClass(DefaultTestEmbeddedCacheManagerProducer.class)
                  .addAsManifestResource(JCache.class.getResource("/META-INF/beans.xml"), "beans.xml")
                  .addAsManifestResource(JCache.class.getResource("/META-INF/services/javax.enterprise.inject.spi.Extension"), "services/javax.enterprise.inject.spi.Extension")
                  .addAsManifestResource(JCache.class.getResource("/META-INF/services/javax.cache.spi.CachingProvider"), "services/javax.cache.spi.CachingProvider")
                  );
   }
}