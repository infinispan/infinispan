package org.infinispan.test.integration.cdi;

import static org.junit.Assert.assertNotEquals;

import jakarta.inject.Inject;

import org.infinispan.AdvancedCache;
import org.infinispan.manager.DefaultCacheManager;
import org.junit.Test;

/**
 * Tests whether {@link DefaultCacheManager} sets custom Cache name to avoid JMX
 * name collision.
 *
 * @author Sebastian Laskawiec
 */
public abstract class AbstractDuplicatedDomainsCdiIT {

   @Inject
   private AdvancedCache<Object, Object> greetingCache;

   /**
    * Creates new {@link DefaultCacheManager}.
    * This test will fail if CDI Extension registers and won't set Cache Manager's name.
    */
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
