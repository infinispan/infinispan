package org.infinispan.loaders.decorators;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.decorators.ReadOnlyCacheStoreTest")
public class ReadOnlyCacheStoreTest extends AbstractInfinispanTest {
   public void testWriteMethods() throws CacheLoaderException {
      CacheStore mock = mock(CacheStore.class);
      ReadOnlyStore store = new ReadOnlyStore(mock);
      InternalCacheEntry mockEntry = mock(InternalCacheEntry.class);
      when(mock.load("key")).thenReturn(mockEntry);

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

      verify(mock).load(eq("key"));
   }
}
