package org.infinispan.configuration;


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

@Test(testName = "config.DifferentCacheModesTest", groups = "unit")
public class DifferentCacheModesTest extends AbstractInfinispanTest {

   public void testCacheModes() throws IOException {
      EmbeddedCacheManager cm = null;
      try {
         String xml = "<infinispan>" +
                 "<global><transport /></global>" +
                 "<default><clustering mode=\"repl\"><sync /></clustering></default>" +
                 "<namedCache name=\"local\"><clustering mode=\"local\" /></namedCache>" +
                 "<namedCache name=\"dist\"><clustering mode=\"dist\"><sync /></clustering></namedCache>" +
                 "<namedCache name=\"distasync\"><clustering mode=\"distribution\"><async /></clustering></namedCache>" +
                 "<namedCache name=\"replicationasync\"><clustering mode=\"replication\"><async /></clustering></namedCache>" +
                 "</infinispan>";

         InputStream is = new ByteArrayInputStream(xml.getBytes());

         cm = TestCacheManagerFactory.fromStream(is);

         GlobalConfiguration gc = cm.getCacheManagerConfiguration();
         Configuration defaultCfg = cm.getCache().getCacheConfiguration();

         assert gc.transport().transport() != null;
         assert defaultCfg.clustering().cacheMode() == CacheMode.REPL_SYNC;

         Configuration cfg = cm.getCache("local").getCacheConfiguration();
         assert cfg.clustering().cacheMode() == CacheMode.LOCAL;

         cfg = cm.getCache("dist").getCacheConfiguration();
         assert cfg.clustering().cacheMode() == CacheMode.DIST_SYNC;

         cfg = cm.getCache("distasync").getCacheConfiguration();
         assert cfg.clustering().cacheMode() == CacheMode.DIST_ASYNC;

         cfg = cm.getCache("replicationasync").getCacheConfiguration();
         assert cfg.clustering().cacheMode() == CacheMode.REPL_ASYNC;
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testReplicationAndStateTransfer() throws IOException {
      EmbeddedCacheManager cm = null;
      try {
         String xml =
            "<infinispan>" +
               "<global><transport /></global>" +
               "<default><clustering mode=\"repl\"><sync /></clustering></default>" +
               "<namedCache name=\"explicit-state-disable\">" +
                  "<clustering mode=\"repl\">" +
                     "<sync />" +
                     "<stateTransfer fetchInMemoryState=\"false\"/>" +
                  "</clustering>" +
               "</namedCache>" +
               "<namedCache name=\"explicit-state-enable\">" +
                  "<clustering mode=\"repl\">" +
                     "<sync />" +
                     "<stateTransfer fetchInMemoryState=\"true\"/>" +
                  "</clustering>" +
               "</namedCache>" +
               "<namedCache name=\"explicit-state-enable-async\">" +
                  "<clustering mode=\"repl\">" +
                     "<async />" +
                     "<stateTransfer fetchInMemoryState=\"true\"/>" +
                  "</clustering>" +
               "</namedCache>" +
             "</infinispan>";

         InputStream is = new ByteArrayInputStream(xml.getBytes());
         cm = TestCacheManagerFactory.fromStream(is);
         GlobalConfiguration gc = cm.getCacheManagerConfiguration();
         Configuration defaultCfg = cm.getCache().getCacheConfiguration();

         assert defaultCfg.clustering().cacheMode() == CacheMode.REPL_SYNC;
         assert defaultCfg.clustering().stateTransfer().fetchInMemoryState();

         Configuration explicitDisable =
               cm.getCache("explicit-state-disable").getCacheConfiguration();
         assert explicitDisable.clustering().cacheMode() == CacheMode.REPL_SYNC;
         assert !explicitDisable.clustering().stateTransfer().fetchInMemoryState();

         Configuration explicitEnable =
               cm.getCache("explicit-state-enable").getCacheConfiguration();
         assert explicitEnable.clustering().cacheMode() == CacheMode.REPL_SYNC;
         assert explicitEnable.clustering().stateTransfer().fetchInMemoryState();

         Configuration explicitEnableAsync =
               cm.getCache("explicit-state-enable-async").getCacheConfiguration();
         assert explicitEnableAsync.clustering().cacheMode() == CacheMode.REPL_ASYNC;
         assert explicitEnableAsync.clustering().stateTransfer().fetchInMemoryState();
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
}
