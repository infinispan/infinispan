package org.infinispan.jmx;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Test(groups = "functional", testName = "jmx.CustomMBeanServerPropertiesTest")
public class CustomMBeanServerPropertiesTest extends AbstractInfinispanTest {
   public void testDeclarativeCustomMBeanServerLookupProperties() throws IOException {
      String cfg = "<infinispan>" +
              "<global>" +
              "<globalJmxStatistics enabled=\"true\" mBeanServerLookup=\"" + TestLookup.class.getName() + "\">" +
              "<properties>" +
              "<property name=\"key\" value=\"value\"/>" +
              "</properties>" +
              "</globalJmxStatistics>" +
              "</global>" +
              "<default><jmxStatistics enabled=\"true\"/></default>" +
              "</infinispan>";
      InputStream stream = new ByteArrayInputStream(cfg.getBytes());
      CacheContainer cc = null;
      try {
         cc = TestCacheManagerFactory.fromStream(stream);
         cc.getCache();
         assert "value".equals(TestLookup.p.get("key"));
      } finally {
         TestingUtil.killCacheManagers(cc);
      }
   }

   public void testProgrammaticCustomMBeanServerLookupProperties() throws IOException {
      CacheContainer cc = null;
      try {
         GlobalConfiguration gc = new GlobalConfiguration();
         TestLookup mbsl = new TestLookup();
         gc.setMBeanServerLookupInstance(mbsl);
         Properties p = new Properties();
         p.setProperty("key", "value");
         gc.setMBeanServerProperties(p);
         Configuration cfg = new Configuration();
         cfg.setExposeJmxStatistics(true);
         cc = TestCacheManagerFactory.createCacheManager(gc, cfg);
         cc.getCache();
         assert "value".equals(mbsl.localProps.get("key"));
      } finally {
         TestingUtil.killCacheManagers(cc);
      }
   }

   public static class TestLookup implements MBeanServerLookup {

      static Properties p;
      Properties localProps;

      @Override
      public MBeanServer getMBeanServer(Properties properties) {
         TestLookup.p = properties;
         localProps = properties;
         return new PerThreadMBeanServerLookup().getMBeanServer(p);
      }
   }
}


