package org.infinispan.scattered;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public abstract class BaseScatteredTest extends MultipleCacheManagersTest {
   protected EmbeddedCacheManager cm1, cm2, cm3;
   protected ControlledScatteredVersionManager[] svms;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.SCATTERED_SYNC, false);
      if (biasAcquisition != null) {
         cfg.clustering().biasAcquisition(biasAcquisition);
      }
      cfg.clustering().hash().numSegments(16);
      cfg.clustering().remoteTimeout(1, TimeUnit.DAYS); // for debugging
      cm1 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      cm2 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      cm3 = TestCacheManagerFactory.createClusteredCacheManager(cfg);
      registerCacheManager(cm1, cm2, cm3);
      // Starting caches and rewiring SVM is not synchronized properly, so we have to first start
      // them and then rewire
      for (EmbeddedCacheManager cm : cacheManagers) cm.getCache();
      TestingUtil.waitForNoRebalance(caches());

      svms = caches().stream().map(c -> {
         ControlledScatteredVersionManager csvm = new ControlledScatteredVersionManager();
         BasicComponentRegistry bcr = c.getAdvancedCache().getComponentRegistry().getComponent(BasicComponentRegistry.class);
         bcr.replaceComponent(ScatteredVersionManager.class.getName(), csvm, true);
         bcr.rewire();
         return csvm;
      }).toArray(ControlledScatteredVersionManager[]::new);
   }

   protected ControlledScatteredVersionManager svm(int node) {
      return svms[node];
   }

   protected void flush(boolean expectRemoval) {
      ControlledScatteredVersionManager.flush(expectRemoval, svms);
   }

   protected DataContainer<Object, Object> dc(int index) {
      return cache(index).getAdvancedCache().getDataContainer();
   }
}
