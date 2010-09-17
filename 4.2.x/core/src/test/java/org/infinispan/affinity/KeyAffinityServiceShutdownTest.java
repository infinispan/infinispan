package org.infinispan.affinity;

import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static junit.framework.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (groups = "functional", testName = "affinity.KeyAffinityServiceShutdownTest")
public class KeyAffinityServiceShutdownTest extends BaseKeyAffinityServiceTest {
   private ExecutorService executor;
   private EmbeddedCacheManager cacheManager;

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();

      ThreadFactory tf = new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            return new Thread(r, "KeyGeneratorThread");
         }
      };
      executor = Executors.newSingleThreadExecutor(tf);
      cacheManager = manager(0);
      keyAffinityService = (KeyAffinityServiceImpl) KeyAffinityServiceFactory.newKeyAffinityService(cacheManager.getCache(cacheName),
                                                                                                    executor,
                                                                                                    new RndKeyGenerator(), 100);
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
