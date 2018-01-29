package org.infinispan.functional;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.CountingRpcManager;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "functional.FunctionalNoopDoesNotGoToBackupTest")
public class FunctionalNoopDoesNotGoToBackupTest extends MultipleCacheManagersTest {
   private ReadWriteMap<Object, Object> rw0, rw1;
   private WriteOnlyMap<Object, Object> wo0, wo1;
   private MagicKey key;
   private CountingRpcManager rpcManager0;
   private CountingRpcManager rpcManager1;

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC), 2);
      FunctionalMapImpl<Object, Object> fmap0 = FunctionalMapImpl.create(cache(0).getAdvancedCache());
      FunctionalMapImpl<Object, Object> fmap1 = FunctionalMapImpl.create(cache(1).getAdvancedCache());
      rw0 = ReadWriteMapImpl.create(fmap0);
      rw1 = ReadWriteMapImpl.create(fmap1);
      wo0 = WriteOnlyMapImpl.create(fmap0);
      wo1 = WriteOnlyMapImpl.create(fmap1);
      key = new MagicKey(cache(0));
      rpcManager0 = TestingUtil.wrapComponent(cache(0), RpcManager.class, CountingRpcManager::new);
      rpcManager1 = TestingUtil.wrapComponent(cache(1), RpcManager.class, CountingRpcManager::new);
   }

   @DataProvider(name = "ownerAndExistence")
   public Object[][] ownerAndExistence() {
      return new Object[][]{
            {false, false},
            {false, true},
            {true, false},
            {true, true}
      };
   }

   @Test(dataProvider = "ownerAndExistence")
   public void testReadWriteKeyCommand(boolean onOwner, boolean withExisting) {
      ReadWriteMap<Object, Object> rw = onOwner ? rw0 : rw1;
      test(() -> rw.eval(key, view -> view.find().orElse(null)), onOwner, withExisting);
   }

   @Test(dataProvider = "ownerAndExistence")
   public void testReadWriteKeyValueCommand(boolean onOwner, boolean withExisting) {
      ReadWriteMap<Object, Object> rw = onOwner ? rw0 : rw1;
      test(() -> rw.eval(key, "bar", (bar, view) -> view.find().orElse(null)), onOwner, withExisting);
   }

   // TODO: Updating routing logic for multi-commands, not sending the update to backup is non-trivial
   @Test(dataProvider = "ownerAndExistence", enabled = false, description = "ISPN-8676")
   public void testReadWriteManyCommand(boolean onOwner, boolean withExisting) {
      ReadWriteMap<Object, Object> rw = onOwner ? rw0 : rw1;
      testMany(() -> rw.evalMany(Collections.singleton(key), view -> view.find().orElse(null)), onOwner, withExisting);
   }

   @Test(dataProvider = "ownerAndExistence", enabled = false, description = "ISPN-8676")
   public void testReadWriteManyEntriesCommand(boolean onOwner, boolean withExisting) {
      ReadWriteMap<Object, Object> rw = onOwner ? rw0 : rw1;
      testMany(() -> rw.evalMany(Collections.singletonMap(key, "bar"), (bar, view) -> view.find().orElse(null)), onOwner, withExisting);
   }

   @Test(dataProvider = "ownerAndExistence")
   public void testWriteOnlyKeyCommand(boolean onOwner, boolean withExisting) {
      WriteOnlyMap<Object, Object> wo = onOwner ? wo0 : wo1;
      test(() -> wo.eval(key, view -> {
      }), onOwner, withExisting);
   }

   @Test(dataProvider = "ownerAndExistence")
   public void testWriteOnlyKeyValueCommand(boolean onOwner, boolean withExisting) {
      WriteOnlyMap<Object, Object> wo = onOwner ? wo0 : wo1;
      test(() -> wo.eval(key, "bar", (bar, view) -> {
      }), onOwner, withExisting);
   }

   @Test(dataProvider = "ownerAndExistence", enabled = false, description = "ISPN-8676")
   public void testWriteOnlyManyCommand(boolean onOwner, boolean withExisting) {
      WriteOnlyMap<Object, Object> wo = onOwner ? wo0 : wo1;
      test(() -> wo.evalMany(Collections.singleton(key), view -> {
      }), onOwner, withExisting);
   }

   @Test(dataProvider = "ownerAndExistence", enabled = false, description = "ISPN-8676")
   public void testWriteOnlyManyEntriesCommand(boolean onOwner, boolean withExisting) {
      WriteOnlyMap<Object, Object> wo = onOwner ? wo0 : wo1;
      test(() -> wo.evalMany(Collections.singletonMap(key, "bar"), (bar, view) -> {
      }), onOwner, withExisting);
   }

   private void test(Supplier<CompletableFuture<?>> supplier, boolean onOwner, boolean withExisting) {
      if (withExisting) {
         cache(0).put(key, "foo");
      }
      rpcManager0.otherCount = 0;
      rpcManager1.otherCount = 0;
      supplier.get().join();
      assertEquals(0, rpcManager0.otherCount);
      assertEquals(onOwner ? 0 : 1, rpcManager1.otherCount);
   }

   private void testMany(Supplier<Traversable<?>> supplier, boolean onOwner, boolean withExisting) {
      if (withExisting) {
         cache(0).put(key, "foo");
      }
      rpcManager0.otherCount = 0;
      rpcManager1.otherCount = 0;
      supplier.get().forEach(x -> {
      });
      assertEquals(0, rpcManager0.otherCount);
      assertEquals(onOwner ? 0 : 1, rpcManager1.otherCount);
   }

}
