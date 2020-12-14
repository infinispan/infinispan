package org.infinispan.remoting.jgroups;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(testName = "remoting.jgroups.NonExistingJGroupsConfigTest", groups = "functional")
public class NonExistingJGroupsConfigTest extends AbstractInfinispanTest {

   public void channelLookupTest() {
      String config = "<infinispan>\n" +
         "<jgroups>\n" +
         "   <stack-file name=\"dummy\" path=\"nosuchfile.xml\"/>\n" +
         "</jgroups>\n" +
         "<cache-container default-cache=\"default\">" +
         "   <transport stack=\"dummy\" cluster=\"demoCluster\" />\n" +
         "   <replicated-cache name=\"default\" />\n" +
         "</cache-container>\n" +
         "</infinispan>";
      Exceptions.expectException(CacheConfigurationException.class,
                                 "ISPN000365:.*", () -> {
               EmbeddedCacheManager cm = null;
               try {
                  ConfigurationBuilderHolder cbh = new ParserRegistry().parse(
                        new ByteArrayInputStream(config.getBytes()), Void -> {
                           throw new FileNotFoundException();
                        }, MediaType.APPLICATION_XML);
                  cm = new DefaultCacheManager(cbh, true);
               } finally {
                  TestingUtil.killCacheManagers(cm);
               }
            });
   }


   public void brokenJGroupsConfigTest() {
      String config = "<infinispan>\n" +
         "<jgroups>\n" +
         "   <stack-file name=\"dummy\" path=\"stacks/broken-tcp.xml\"/>\n" +
         "</jgroups>\n" +
         "<cache-container default-cache=\"default\">" +
         "   <transport stack=\"dummy\" cluster=\"demoCluster\" />\n" +
         "   <replicated-cache name=\"default\" />\n" +
         "</cache-container>\n" +
         "</infinispan>";
      Exceptions.expectException(EmbeddedCacheManagerStartupException.class,
                                 CacheConfigurationException.class,
                                 "ISPN000541:.*", () -> {
               EmbeddedCacheManager cm = null;
               try {
                  cm = new DefaultCacheManager(new ByteArrayInputStream(config.getBytes()));
               } finally {
                  TestingUtil.killCacheManagers(cm);
               }
            });
   }
}
