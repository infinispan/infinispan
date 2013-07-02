package org.infinispan.config;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

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

         GlobalConfiguration gc = cm.getGlobalConfiguration();
         Configuration defaultCfg = cm.getCache().getConfiguration();

         assert gc.getTransportClass() != null;
         assert defaultCfg.getCacheMode() == Configuration.CacheMode.REPL_SYNC;

         Configuration cfg = cm.getCache("local").getConfiguration();
         assert cfg.getCacheMode() == Configuration.CacheMode.LOCAL;

         cfg = cm.getCache("dist").getConfiguration();
         assert cfg.getCacheMode() == Configuration.CacheMode.DIST_SYNC;

         cfg = cm.getCache("distasync").getConfiguration();
         assert cfg.getCacheMode() == Configuration.CacheMode.DIST_ASYNC;

         cfg = cm.getCache("replicationasync").getConfiguration();
         assert cfg.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
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
         GlobalConfiguration gc = cm.getGlobalConfiguration();
         Configuration defaultCfg = cm.getCache().getConfiguration();

         assert defaultCfg.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
         assert defaultCfg.isStateTransferEnabled();

         Configuration explicitDisable =
               cm.getCache("explicit-state-disable").getConfiguration();
         assert explicitDisable.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
         assert !explicitDisable.isStateTransferEnabled();

         Configuration explicitEnable =
               cm.getCache("explicit-state-enable").getConfiguration();
         assert explicitEnable.getCacheMode() == Configuration.CacheMode.REPL_SYNC;
         assert explicitEnable.isStateTransferEnabled();

         Configuration explicitEnableAsync =
               cm.getCache("explicit-state-enable-async").getConfiguration();
         assert explicitEnableAsync.getCacheMode() == Configuration.CacheMode.REPL_ASYNC;
         assert explicitEnableAsync.isStateTransferEnabled();
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
}
