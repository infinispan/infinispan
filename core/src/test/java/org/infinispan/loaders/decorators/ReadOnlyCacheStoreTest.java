package org.infinispan.loaders.decorators;

import static org.easymock.EasyMock.*;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.decorators.ReadOnlyCacheStoreTest")
public class ReadOnlyCacheStoreTest {
   public void testWriteMethods() throws CacheLoaderException {
      CacheStore mock = createMock(CacheStore.class);
      ReadOnlyStore store = new ReadOnlyStore(mock);
      InternalCacheEntry mockEntry = createNiceMock(InternalCacheEntry.class);
      expect(mock.load(eq("key"))).andReturn(mockEntry).once();
      replay(mock);

      // these should be "silent" no-ops and not actually change anything.
      store.clear();
      store.purgeExpired();
      store.remove("key");
      store.store(null);
      store.fromStream(null);
      store.prepare(null, null, true);
      store.commit(null);
      store.rollback(null);
      assert mockEntry == store.load("key");

      verify(mock);
   }
}
