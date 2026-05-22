package org.infinispan.test.integration.thirdparty.cdi;

import static org.infinispan.test.integration.thirdparty.DeploymentHelper.addLibrary;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.infinispan.AdvancedCache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.integration.thirdparty.DeploymentHelper;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit5.container.annotation.ArquillianTest;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.jupiter.api.Test;

import jakarta.inject.Inject;

/**
 * Tests whether {@link DefaultCacheManager} sets custom Cache name to avoid JMX
 * name collision.
 *
 * @author Sebastian Laskawiec
 */
@ArquillianTest
public class DuplicatedDomainsCdiIT {

   @Inject
   private AdvancedCache<Object, Object> greetingCache;

   @Deployment
   @TargetsContainer("server-1")
   public static Archive<?> deployment() {
      WebArchive war = DeploymentHelper.createDeployment();
      addLibrary(war, "org.infinispan:infinispan-cdi-embedded");
      return war;
   }

   @Test
   public void testIfCreatingDefaultCacheManagerSucceeds() {
      greetingCache.put("test-key", "test-value");

      String cdiName = greetingCache.getCacheManager().getCacheManagerInfo().getName();

      DefaultCacheManager cacheManager = new DefaultCacheManager();
      String defaultName = cacheManager.getName();
      cacheManager.stop();

      assertNotEquals(defaultName, cdiName);
   }
}
