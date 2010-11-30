package org.infinispan.config;


import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

@Test(testName = "config.DifferentCacheModesTest", groups = "unit")
public class DifferentCacheModesTest extends AbstractInfinispanTest {

   public void testCacheModes() throws IOException {
      EmbeddedCacheManager cm = null;
      try {
         String xml = "<infinispan>" +
                 "<global><transport /></global>" +
                 "<default><clustering mode=\"r\"><sync /></clustering></default>" +
                 "<namedCache name=\"local\"><clustering mode=\"local\" /></namedCache>" +
                 "<namedCache name=\"dist\"><clustering mode=\"d\"><sync /></clustering></namedCache>" +
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
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
}
