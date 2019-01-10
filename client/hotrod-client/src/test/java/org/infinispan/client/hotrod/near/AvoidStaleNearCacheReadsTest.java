package org.infinispan.client.hotrod.near;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.AvoidStaleNearCacheReadsTest")
public class AvoidStaleNearCacheReadsTest extends SingleHotRodServerTest {

   @AfterMethod(alwaysRun=true)
   @Override
   protected void clearContent() {
      super.clearContent();
      remoteCacheManager.getCache().clear(); // Clear the near cache too
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      builder.nearCache().mode(NearCacheMode.INVALIDATED).maxEntries(-1);
      return new RemoteCacheManager(builder.build());
   }

   public void testAvoidStaleReadsAfterPutRemove() {
      repeated((i, remote) -> {
         String value = "v" + i;
         remote.put(1, value);
         assertEquals(value, remote.get(1));
         remote.remove(1);
         assertNull(remote.get(1));
      });
   }

   public void testAvoidStaleReadsAfterPutAll() {
      repeated((i, remote) -> {
         String value = "v" + i;
         Map<Integer, String> map = new HashMap<>();
         map.put(1, value);
         remote.putAll(map);
         assertEquals(value, remote.get(1));
      });
   }

   public void testAvoidStaleReadsAfterReplace() {
      repeated((i, remote) -> {
         String value = "v" + i;
         remote.replace(1, value);
         VersionedValue<String> versioned = remote.getVersioned(1);
         assertEquals(value, versioned.getValue());
      });
   }

   public void testAvoidStaleReadsAfterReplaceWithVersion() {
      repeated((i, remote) -> {
         String value = "v" + i;
         VersionedValue<String> versioned = remote.getVersioned(1);
         remote.replaceWithVersion(1, value, versioned.getVersion());
         assertEquals(value, remote.get(1));
      });
   }

   public void testAvoidStaleReadsAfterPutAsyncRemoveVersioned() {
      repeated((i, remote) -> {
         String value = "v" + i;
         await(remote.putAsync(1, value));
         VersionedValue<String> versioned = remote.getVersioned(1);
         assertEquals(value, versioned.getValue());
         remote.removeWithVersion(1, versioned.getVersion());
         assertNull(remote.get(1));
      });
   }

   private void repeated(BiConsumer<Integer, RemoteCache<Integer, String>> c) {
      RemoteCache<Integer, String> remote = remoteCacheManager.getCache();
      remote.putIfAbsent(1, "v0");
      IntStream.range(1, 1000).forEach(i -> {
         c.accept(i, remote);
      });
   }

   static <T> T await(Future<T> f) {
      try {
         return f.get(10000, TimeUnit.SECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e ) {
         throw new AssertionError(e);
      }
   }

}
