package org.infinispan.remoting.jgroups;

import static org.testng.AssertJUnit.fail;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(testName = "remoting.jgroups.NonExistingJGroupsConfigTest", groups = "functional")
public class NonExistingJGroupsConfigTest extends AbstractInfinispanTest {

   public void channelLookupTest() throws Throwable {
      String config = "<infinispan>\n" +
         "<jgroups>\n" +
         "   <stack-file name=\"dummy\" path=\"nosuchfile.xml\"/>\n" +
         "</jgroups>\n" +
         "<cache-container default-cache=\"default\">" +
         "   <transport stack=\"dummy\" cluster=\"demoCluster\" />\n" +
         "   <replicated-cache name=\"default\" />\n" +
         "</cache-container>\n" +
         "</infinispan>";
      EmbeddedCacheManager cm = null;
      try {
         ConfigurationBuilderHolder cbh = new ParserRegistry().parse(new ByteArrayInputStream(config.getBytes()), Void -> {
            throw new FileNotFoundException();
         });
         cm = new DefaultCacheManager(cbh, true);
         cm.getCache();
         fail("CacheManager construction should have failed");
      } catch (Exception e) {
         TestingUtil.expectCause(e, CacheConfigurationException.class, "ISPN000365:.*");
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }


   public void brokenJGroupsConfigTest() throws Throwable {
      String config = "<infinispan>\n" +
         "<jgroups>\n" +
         "   <stack-file name=\"dummy\" path=\"stacks/broken-tcp.xml\"/>\n" +
         "</jgroups>\n" +
         "<cache-container default-cache=\"default\">" +
         "   <transport stack=\"dummy\" cluster=\"demoCluster\" />\n" +
         "   <replicated-cache name=\"default\" />\n" +
         "</cache-container>\n" +
         "</infinispan>";
      EmbeddedCacheManager cm = null;
      try {
         cm = new DefaultCacheManager(new ByteArrayInputStream(config.getBytes()));
         cm.getCache();
         fail("CacheManager construction should have failed");
      } catch (Exception e) {
         TestingUtil.expectCause(e, CacheConfigurationException.class, "ISPN000541:.*");
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
}
