package org.infinispan.api;

import static org.testng.AssertJUnit.assertEquals;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "functional", testName = "api.StartCacheFromListenerTest")
public class StartCacheFromListenerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager();
      addClusterEnabledCacheManager();
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      manager(0).defineConfiguration("some", dcc.build());
      manager(0).defineConfiguration("cacheStarting", dcc.build());
      manager(0).defineConfiguration("cacheStarted", dcc.build());
   }

   final AtomicBoolean cacheStartingInvoked = new AtomicBoolean(false);

   public void testSingleInvocation() {
      final EmbeddedCacheManager cacheManager = manager(0);
      GlobalComponentRegistry registry = (GlobalComponentRegistry) TestingUtil.extractField(cacheManager, "globalComponentRegistry");
      List<ModuleLifecycle> lifecycles = new LinkedList<ModuleLifecycle>();
      TestingUtil.replaceField(lifecycles, "moduleLifecycles", registry, GlobalComponentRegistry.class);
      lifecycles.add(new ModuleLifecycle() {
         @Override
         public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
            log.debug("StartCacheFromListenerTest.cacheStarting");
            if (!cacheStartingInvoked.get()) {
               cacheStartingInvoked.set(true);
               Future<Cache> fork = fork(new Callable<Cache>() {
                  @Override
                  public Cache call() throws Exception {
                     try {
                        return cacheManager.getCache("cacheStarting");
                     } catch (Exception e) {
                        log.error("Got", e);
                        throw e;
                     }
                  }
               });
               try {
                  log.debug("About to wait in get");
                  Cache cache = fork.get();
                  cache.put("k", "v");
                  log.debug("returned from get!");
               } catch (InterruptedException e) {
                  log.error("Interrupted while waiting for the cache to start");
               } catch (ExecutionException e) {
                  log.error("Failed to start cache", e);
               }
            }
         }
      });

      log.debug("StartCacheFromListenerTest.testSingleInvocation1");
      Cache<Object, Object> some = cacheManager.getCache("some");
      log.debug("StartCacheFromListenerTest.testSingleInvocation2");
      some.put("k", "v");

      assertEquals("v", cacheManager.getCache("cacheStarting").get("k"));
   }

   public void testStartSameCache() {
      final EmbeddedCacheManager cacheManager = manager(0);
      GlobalComponentRegistry registry = (GlobalComponentRegistry) TestingUtil.extractField(cacheManager, "globalComponentRegistry");
      List<ModuleLifecycle> lifecycles = new LinkedList<>();
      TestingUtil.replaceField(lifecycles, "moduleLifecycles", registry, GlobalComponentRegistry.class);
      lifecycles.add(new ModuleLifecycle() {
         @Override
         public void cacheStarted(ComponentRegistry cr,  String cacheName) {
            Cache cache = cacheManager.getCache("single");
            cache.put("k1", "v1");
         }
      });

      Cache<Object, Object> some = cacheManager.getCache("single");
      some.put("k2", "v2");

      assertEquals("v1", cacheManager.getCache("single").get("k1"));
      assertEquals("v2", cacheManager.getCache("single").get("k2"));
   }
}
