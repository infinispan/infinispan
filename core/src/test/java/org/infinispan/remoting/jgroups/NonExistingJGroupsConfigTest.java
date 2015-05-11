package org.infinispan.remoting.jgroups;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.*;

import java.io.ByteArrayInputStream;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.TestingUtil.InfinispanStartTag;
import org.testng.annotations.Test;

@Test(testName = "remoting.jgroups.NonExistingJGroupsConfigTest", groups = "functional")
public class NonExistingJGroupsConfigTest extends AbstractInfinispanTest {

   public void channelLookupTest() throws Throwable {
      String config = InfinispanStartTag.LATEST +
         "<jgroups>\n" +
         "   <stack-file name=\"dummy\" path=\"nosuchfile.xml\"/>\n" +
         "</jgroups>\n" +
         "<cache-container default-cache=\"default\">" +
         "   <jmx domain=\"NonExistingJGroupsConfigTest_channelLookupTest\" />\n" +
         "   <transport stack=\"dummy\" cluster=\"demoCluster\" />\n" +
         "   <replicated-cache name=\"default\" />\n" +
         "</cache-container>" +
         TestingUtil.INFINISPAN_END_TAG;
      EmbeddedCacheManager cm = null;
      try {
         cm = new DefaultCacheManager(new ByteArrayInputStream(config.getBytes()));
         cm.getCache();
         fail("CacheManager construction should have failed");
      } catch (Exception e) {
         TestingUtil.expectCause(e, CacheConfigurationException.class, "ISPN000365:.*");
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }


   public void brokenJGroupsConfigTest() throws Throwable {
      String config = InfinispanStartTag.LATEST +
         "<jgroups>\n" +
         "   <stack-file name=\"dummy\" path=\"stacks/broken-tcp.xml\"/>\n" +
         "</jgroups>\n" +
         "<cache-container default-cache=\"default\">" +
         "   <jmx domain=\"NonExistingJGroupsConfigTest_brokenJGroupsConfigTest\" />\n" +
         "   <transport stack=\"dummy\" cluster=\"demoCluster\" />\n" +
         "   <replicated-cache name=\"default\" />\n" +
         "</cache-container>" +
         TestingUtil.INFINISPAN_END_TAG;
      EmbeddedCacheManager cm = null;
      try {
         cm = new DefaultCacheManager(new ByteArrayInputStream(config.getBytes()));
         cm.getCache();
         fail("CacheManager construction should have failed");
      } catch (Exception e) {
         TestingUtil.expectCause(e, CacheConfigurationException.class, "ISPN000085:.*");
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }
}
