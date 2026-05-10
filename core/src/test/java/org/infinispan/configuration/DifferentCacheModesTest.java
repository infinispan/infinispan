package org.infinispan.configuration;


import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(testName = "configuration.DifferentCacheModesTest", groups = "unit")
public class DifferentCacheModesTest extends AbstractInfinispanTest {

   public void testCacheModes() {
      EmbeddedCacheManager cm = null;
      try {
         String xml = "<infinispan>" +
                 "<jgroups/>" +
                 "<cache-container name=\"different-cache-modes\" default-cache=\"replicated\">" +
                 "<replicated-cache name=\"replicated\" mode=\"SYNC\"></replicated-cache>" +
                 "<local-cache name=\"local\"></local-cache>" +
                 "<distributed-cache name=\"dist\" mode=\"SYNC\"></distributed-cache>" +
                 "<distributed-cache name=\"distasync\" mode=\"ASYNC\"></distributed-cache>" +
                 "<replicated-cache name=\"replicationasync\" mode=\"ASYNC\"></replicated-cache>" +
                 "</cache-container>" +
                 "</infinispan>";

         InputStream is = new ByteArrayInputStream(xml.getBytes());

         cm = TestCacheManagerFactory.fromStream(is);

         GlobalConfiguration gc = cm.getCacheManagerConfiguration();
         Configuration defaultCfg = cm.getCache().getCacheConfiguration();

         assertNotNull(gc.transport().transport());
         assertTrue(defaultCfg.clustering().cacheMode() == CacheMode.REPL_SYNC);

         Configuration cfg = cm.getCache("local").getCacheConfiguration();
         assertTrue(cfg.clustering().cacheMode() == CacheMode.LOCAL);

         cfg = cm.getCache("dist").getCacheConfiguration();
         assertTrue(cfg.clustering().cacheMode() == CacheMode.DIST_SYNC);

         cfg = cm.getCache("distasync").getCacheConfiguration();
         assertTrue(cfg.clustering().cacheMode() == CacheMode.DIST_ASYNC);

         cfg = cm.getCache("replicationasync").getCacheConfiguration();
         assertTrue(cfg.clustering().cacheMode() == CacheMode.REPL_ASYNC);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testReplicationAndStateTransfer() throws IOException {
      EmbeddedCacheManager cm = null;
      try {
         String xml =
            "<infinispan>" +
               "<jgroups/>" +
               "<cache-container name=\"different-cache-modes\" default-cache=\"replicated\">" +
               "<replicated-cache name=\"replicated\" mode=\"SYNC\"/>" +
               "<replicated-cache name=\"explicit-state-disable\" mode=\"SYNC\">" +
                  "<state-transfer enabled=\"false\"/>" +
               "</replicated-cache>" +
               "<replicated-cache name=\"explicit-state-enable\" mode=\"SYNC\">" +
                  "<state-transfer enabled=\"true\"/>" +
               "</replicated-cache>" +
               "<replicated-cache name=\"explicit-state-enable-async\" mode=\"ASYNC\">" +
                  "<state-transfer enabled=\"true\"/>" +
               "</replicated-cache>" +
               "</cache-container>" +
            "</infinispan>";

         InputStream is = new ByteArrayInputStream(xml.getBytes());
         cm = TestCacheManagerFactory.fromStream(is);
         GlobalConfiguration gc = cm.getCacheManagerConfiguration();
         Configuration defaultCfg = cm.getCache().getCacheConfiguration();

         assertTrue(defaultCfg.clustering().cacheMode() == CacheMode.REPL_SYNC);
         assertTrue(defaultCfg.clustering().stateTransfer().fetchInMemoryState());

         Configuration explicitDisable =
               cm.getCache("explicit-state-disable").getCacheConfiguration();
         assertTrue(explicitDisable.clustering().cacheMode() == CacheMode.REPL_SYNC);
         assertFalse(explicitDisable.clustering().stateTransfer().fetchInMemoryState());

         Configuration explicitEnable =
               cm.getCache("explicit-state-enable").getCacheConfiguration();
         assertTrue(explicitEnable.clustering().cacheMode() == CacheMode.REPL_SYNC);
         assertTrue(explicitEnable.clustering().stateTransfer().fetchInMemoryState());

         Configuration explicitEnableAsync =
               cm.getCache("explicit-state-enable-async").getCacheConfiguration();
         assertTrue(explicitEnableAsync.clustering().cacheMode() == CacheMode.REPL_ASYNC);
         assertTrue(explicitEnableAsync.clustering().stateTransfer().fetchInMemoryState());
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
}
