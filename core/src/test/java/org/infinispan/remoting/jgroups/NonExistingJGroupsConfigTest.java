package org.infinispan.remoting.jgroups;

import static org.infinispan.test.TestingUtil.INFINISPAN_START_TAG;
import static org.infinispan.test.TestingUtil.withCacheManager;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(testName = "remoting.jgroups.NonExistingJGroupsConfigTest", groups = "functional")
public class NonExistingJGroupsConfigTest extends AbstractInfinispanTest {
   
   public void channelLookupTest() throws Exception {
      String config = INFINISPAN_START_TAG +
      "<jgroups>\n" +
      "   <stack-file name=\"dummy\" path=\"nosuchfile.xml\"/>\n" +
      "</jgroups>\n" +
      "<cache-container default-cache=\"default\">" +
      "   <transport stack=\"dummy\" cluster=\"demoCluster\" />\n" +
      "   <replicated-cache name=\"default\" />\n" +
      "</cache-container>" +
      TestingUtil.INFINISPAN_END_TAG;

      InputStream is = new ByteArrayInputStream(config.getBytes());
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromStream(is)) {
         @Override
         public void call() {
            try {
               cm.getCache();
            } catch (Exception e) {
               assert e.getCause().getCause().getCause() instanceof FileNotFoundException;
            }
         }
      });
   }
}
