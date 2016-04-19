package org.infinispan.test.integration.as.cdi;

import org.infinispan.AdvancedCache;
import org.infinispan.cdi.embedded.DefaultEmbeddedCacheManagerProducer;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.maven.Maven;
import org.jboss.shrinkwrap.resolver.api.maven.PomEquippedResolveStage;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import java.io.File;

/**
 * Tests whether {@link DefaultEmbeddedCacheManagerProducer} sets custom Cache name to avoid JMX
 * name collision.
 *
 * @author Sebastian Laskawiec
 */
@RunWith(Arquillian.class)
@Ignore
public class DuplicatedDomainsCdiIT {


   @Deployment
   public static Archive<?> deployment() {
      WebArchive webArchive = ShrinkWrap
            .create(WebArchive.class, "cdi.war")
            .addClass(DuplicatedDomainsCdiIT.class)
            .addAsWebInfResource("beans.xml");

      PomEquippedResolveStage mavenResolver = Maven.resolver().loadPomFromFile(new File("pom.xml"));

      webArchive.addAsLibraries(mavenResolver.resolve("org.infinispan:infinispan-jcache").withTransitivity().asFile());

      return webArchive;
   }

   @Inject
   private AdvancedCache<Object, Object> greetingCache;

   @Inject
   DefaultEmbeddedCacheManagerProducer defaultEmbeddedCacheManagerProducer;

   /**
    * Creates new {@link DefaultEmbeddedCacheManagerProducer} with default {@link org.infinispan.configuration.cache.Configuration}.
    * This test will fail if CDI Extension registers and won't set Cache Manager's name.
    */
   @Test
   public void testIfCreatingDefaultCacheManagerSucceeds() {
      DefaultCacheManager cacheManager = new DefaultCacheManager(new ConfigurationBuilder().build());
      cacheManager.stop();
   }

}
