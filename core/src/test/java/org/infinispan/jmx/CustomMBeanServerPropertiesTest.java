package org.infinispan.jmx;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Properties;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "jmx.CustomMBeanServerPropertiesTest")
public class CustomMBeanServerPropertiesTest extends AbstractInfinispanTest {

   public void testDeclarativeCustomMBeanServerLookupProperties() {
      String cfg = "<infinispan>" +
            "<cache-container default-cache=\"default\">" +
            "<jmx enabled=\"true\" mbean-server-lookup=\"" + TestLookup.class.getName() + "\">" +
            "<property name=\"key\">value</property>" +
            "</jmx>" +
            "<local-cache name=\"default\"/>" +
            "</cache-container>" +
            "</infinispan>";
      InputStream stream = new ByteArrayInputStream(cfg.getBytes());

      EmbeddedCacheManager cm = null;
      try {
         cm = TestCacheManagerFactory.fromStream(stream);
         cm.getCache();
         MBeanServerLookup mbsl = cm.getCacheManagerConfiguration().jmx().mbeanServerLookup();
         assertTrue(mbsl instanceof TestLookup);
         assertEquals("value", ((TestLookup) mbsl).props.get("key"));
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public void testProgrammaticCustomMBeanServerLookupProperties() {
      EmbeddedCacheManager cm = null;
      try {
         GlobalConfigurationBuilder gc = new GlobalConfigurationBuilder();
         TestLookup mbsl = new TestLookup();
         gc.jmx().enabled(true).mBeanServerLookup(mbsl).addProperty("key", "value");
         ConfigurationBuilder cfg = new ConfigurationBuilder();
         cm = TestCacheManagerFactory.createCacheManager(gc, cfg);
         cm.getCache();
         assertEquals("value", mbsl.props.get("key"));
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }

   public static final class TestLookup implements MBeanServerLookup {

      Properties props;

      private final MBeanServer mBeanServer = MBeanServerFactory.newMBeanServer();

      @Override
      public MBeanServer getMBeanServer(Properties props) {
         this.props = props;
         return mBeanServer;
      }
   }
}
