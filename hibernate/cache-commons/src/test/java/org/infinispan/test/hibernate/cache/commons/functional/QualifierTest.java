package org.infinispan.test.hibernate.cache.commons.functional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hibernate.cfg.AvailableSettings;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactory;
import org.infinispan.test.hibernate.cache.commons.util.TestRegionFactoryProvider;
import org.infinispan.test.hibernate.cache.commons.util.TestSessionAccess;
import org.junit.Test;

public class QualifierTest extends AbstractFunctionalTest {
   public static final String FOO_BAR = "foo.bar";

   @Override
   public List<Object[]> getParameters() {
      return Collections.singletonList(READ_WRITE_DISTRIBUTED);
   }

   @Override
   protected void addSettings(Map settings) {
      super.addSettings(settings);
      settings.put(AvailableSettings.CACHE_REGION_PREFIX, FOO_BAR);
   }

   @Test
   public void testRegionNamesQualified() {
      TestRegionFactory factory = TestRegionFactoryProvider.INSTANCE.findRegionFactory(sessionFactory().getCache());
      EmbeddedCacheManager cacheManager = factory.getCacheManager();
      for (String cacheName : cacheManager.getCacheNames()) {
         assertTrue(cacheName.startsWith(FOO_BAR));
      }
      // In Hibernate < 5.3 the region factory got qualified names and couldn't use any unqualified form
      if (!TestRegionFactoryProvider.INSTANCE.getRegionFactoryClass().getName().contains(".v51.")) {
         for (InfinispanBaseRegion region : TestSessionAccess.findTestSessionAccess().getAllRegions(sessionFactory())) {
            assertFalse(region.getName().startsWith(FOO_BAR));
         }
      }
   }
}
