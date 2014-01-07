package org.infinispan.jmx;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
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
              "<cache-container default-cache=\"default\">" +
              "<jmx mbean-server-lookup=\"" + TestLookup.class.getName() + "\">" +
              "<property name=\"key\">value</property>" +
              "</jmx>" +
              "<local-cache name=\"default\" statistics=\"true\"/>" +
              "</cache-container>" +
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

   public void testProgrammaticCustomMBeanServerLookupProperties() {
      CacheContainer cc = null;
      try {
         GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
         TestLookup mbsl = new TestLookup();
         gc.globalJmxStatistics().enable().mBeanServerLookup(mbsl);
         Properties p = new Properties();
         p.setProperty("key", "value");
         gc.globalJmxStatistics().addProperty("key", "value");
         ConfigurationBuilder cfg = new ConfigurationBuilder();
         cfg.jmxStatistics().enable();
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


