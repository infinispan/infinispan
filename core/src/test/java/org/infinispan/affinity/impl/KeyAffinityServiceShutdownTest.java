package org.infinispan.affinity.impl;

import org.infinispan.affinity.KeyAffinityServiceFactory;
import org.infinispan.affinity.ListenerRegistration;
import org.infinispan.affinity.impl.KeyAffinityServiceImpl;
import org.infinispan.affinity.impl.RndKeyGenerator;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (groups = "functional", testName = "affinity.KeyAffinityServiceShutdownTest")
public class KeyAffinityServiceShutdownTest extends BaseKeyAffinityServiceTest {
   private EmbeddedCacheManager cacheManager;

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();

      cacheManager = manager(0);
      keyAffinityService = (KeyAffinityServiceImpl<Object>) KeyAffinityServiceFactory.newKeyAffinityService(cacheManager.getCache(cacheName),
            executor, new RndKeyGenerator(), 100);
   }

   public void testSimpleShutdown() throws Exception {
      assertListenerRegistered(true);
      assertEventualFullCapacity();
      assert keyAffinityService.isKeyGeneratorThreadAlive();
      keyAffinityService.stop();
      for (int i = 0; i < 10; i++) {
         if (!keyAffinityService.isKeyGeneratorThreadAlive())
            break;
         Thread.sleep(1000);
      }
      assert !keyAffinityService.isKeyGeneratorThreadAlive();
      assert !executor.isShutdown();
   }

   @Test(dependsOnMethods = "testSimpleShutdown")
   public void testServiceCannotBeUsedAfterShutdown() {
      try {
         keyAffinityService.getKeyForAddress(topology().get(0));
         assert false : "Exception expected!";
      } catch (IllegalStateException e) {
         //expected
      }
      try {
         keyAffinityService.getCollocatedKey("a");
         assert false : "Exception expected!";
      } catch (IllegalStateException e) {
         //expected
      }
   }

   @Test (dependsOnMethods = "testServiceCannotBeUsedAfterShutdown")
   public void testViewChaneListenerUnregistered() {
      assertListenerRegistered(false);
   }

   @Test (dependsOnMethods = "testViewChaneListenerUnregistered")
   public void testRestart() throws InterruptedException {
      keyAffinityService.start();
      assertEventualFullCapacity();
   }

   private void assertListenerRegistered(boolean registered) {
      boolean isRegistered = false;
      for (Object o : cacheManager.getListeners()) {
         if (o instanceof ListenerRegistration) {
            isRegistered = true;
            break;
         }
      }
      assertEquals(registered, isRegistered);
   }   
}
