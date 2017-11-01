package org.infinispan.server.test.partitionhandling;

import static org.junit.Assert.assertNotNull;

import java.io.File;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.server.test.util.ITestUtils;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests deployable Custom merge policies.
 *
 * @author Ryan Emerson
 * @since 9.2
 */
@RunWith(Arquillian.class)
public class CustomEntryMergePolicyIT {

   // Reuse the config of the CustomCacheStoreIT
   @InfinispanResource("clustered-mergepolicies")
   RemoteInfinispanServer server;

   @BeforeClass
   public static void before() throws Exception {
      JavaArchive deployedMergePolicy = ShrinkWrap.create(JavaArchive.class);
      deployedMergePolicy.addPackage(CustomEntryMergePolicy.class.getPackage());
      deployedMergePolicy.addAsServiceProvider(EntryMergePolicy.class, CustomEntryMergePolicy.class);

      deployedMergePolicy.as(ZipExporter.class).exportTo(
            new File(System.getProperty("server1.dist"), "/standalone/deployments/custom-entry-merge-policy.jar"), true);
   }

   @Test
   @WithRunningServer({@RunningServer(name = "clustered-mergepolicies")})
   public void testIfDeployedCacheContainsProperValues() throws Exception {
      // If the custom merge policy does not exist then the cache will not sart
      RemoteCacheManager rcm = ITestUtils.createCacheManager(server);
      assertNotNull(rcm.getCache());
   }
}
