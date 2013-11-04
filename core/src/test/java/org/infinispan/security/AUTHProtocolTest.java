package org.infinispan.security;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

import static org.testng.AssertJUnit.fail;

@Test(groups = "functional", testName = "security.AUTHProtocolTest")
public class AUTHProtocolTest extends MultipleCacheManagersTest {

   public static final String CACHE_NAME = "TestCache";

   public AUTHProtocolTest() {
      cleanup = CleanupPhase.AFTER_METHOD;  // cleanup after each method
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      // we want to create cache managers manually with custom jgroups settings
   }

   private void verifyIfDataCouldBeRetrieved() {
      Cache<String, String> cacheA = cache(0, CACHE_NAME);
      Cache<String, String> cacheB = cache(1, CACHE_NAME);
      cacheA.put("keyA", "valA");
      cacheB.put("keyB", "valB");

      String keyA = cacheB.get("keyA");
      String keyB = cacheA.get("keyB");
      assert keyA.compareTo("valA") == 0;
      assert keyB.compareTo("valB") == 0;
   }

   private static EmbeddedCacheManager createCacheManagerProgramatically(String path) {
      EmbeddedCacheManager cm = new DefaultCacheManager(
            GlobalConfigurationBuilder.defaultClusteredBuilder().globalJmxStatistics().allowDuplicateDomains(true)
                  .transport().addProperty("configurationFile", path)
                  .build(),
            new ConfigurationBuilder()
                  .clustering().cacheMode(getCacheMode())
                  .build()
      );
      return cm;
   }

   public static CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   @Test
   public void testJoiningCacheManagersWithSameCertificateViaTCP() throws ExecutionException, InterruptedException {

      cacheManagers.add(createCacheManagerProgramatically("stacks/security/tcp.xml"));
      cacheManagers.add(createCacheManagerProgramatically("stacks/security/tcp.xml"));

      waitForClusterToForm();

      verifyIfDataCouldBeRetrieved();

      StateTransferManager stm = TestingUtil.extractComponent(cache(0, CACHE_NAME), StateTransferManager.class);
      assert 2 == stm.getCacheTopology().getCurrentCH().getMembers().size();
   }

   @Test
   public void testJoiningCacheManagersWithWrongCertificateViaTCP() throws ExecutionException, InterruptedException {
      try {
         cacheManagers.add(createCacheManagerProgramatically("stacks/security/tcp.xml"));
         cacheManagers.add(createCacheManagerProgramatically("stacks/security/tcp-wrong.xml"));

         waitForClusterToForm();
         fail("Should throw SecurityException");
      } catch (RuntimeException ex) {
         if (!isCause(SecurityException.class, ex)) fail("StackTrace does not contain SecurityException");
      }
   }

   @Test
   public void testJoiningCacheManagersWithSameCertificateViaUDP() throws ExecutionException, InterruptedException {

      cacheManagers.add(createCacheManagerProgramatically("stacks/security/udp.xml"));
      cacheManagers.add(createCacheManagerProgramatically("stacks/security/udp.xml"));

      waitForClusterToForm();

      verifyIfDataCouldBeRetrieved();

      StateTransferManager stm = TestingUtil.extractComponent(cache(0, CACHE_NAME), StateTransferManager.class);
      assert 2 == stm.getCacheTopology().getCurrentCH().getMembers().size();

   }

   @Test
   public void testJoiningCacheManagersWithWrongCertificateViaUDP() throws ExecutionException, InterruptedException {
      try {
         cacheManagers.add(createCacheManagerProgramatically("stacks/security/udp.xml"));
         cacheManagers.add(createCacheManagerProgramatically("stacks/security/udp-wrong.xml"));

         waitForClusterToForm();
         fail("Should throw SecurityException");
      } catch (RuntimeException ex) {
         if (!isCause(SecurityException.class, ex)) fail("StackTrace does not contain SecurityException");
      }
   }

   public static boolean isCause(Class<? extends Throwable> expected, Throwable exception) {
      return expected.isInstance(exception) || (
            exception != null && isCause(expected, exception.getCause())
      );
   }
}

