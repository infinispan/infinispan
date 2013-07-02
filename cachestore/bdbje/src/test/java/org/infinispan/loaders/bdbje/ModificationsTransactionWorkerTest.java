package org.infinispan.loaders.bdbje;

import static org.mockito.Mockito.*;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.modifications.Clear;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.PurgeExpired;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.testng.annotations.Test;

import java.util.Collections;

/**
 * Unit tests that cover {@link  ModificationsTransactionWorker }
 *
 * @author Adrian Cole
 * @since 4.0
 */
@Test(groups = "unit", enabled = true, testName = "loaders.bdbje.ModificationsTransactionWorkerTest")
public class ModificationsTransactionWorkerTest {

   @Test
   public void testDoWorkOnStore() throws Exception {
      CacheStore cs = mock(CacheStore.class);
      Store store = mock(Store.class);
      InternalCacheEntry entry = TestInternalCacheEntryFactory.create("1", "2");
      when(store.getType()).thenReturn(Modification.Type.STORE);
      when(store.getStoredEntry()).thenReturn(entry);
      cs.store(entry);


      ModificationsTransactionWorker worker =
            new ModificationsTransactionWorker(cs,
                                               Collections.singletonList(store));
      worker.doWork();

   }

   @Test
   public void testDoWorkOnRemove() throws Exception {
      CacheStore cs = mock(CacheStore.class);
      Remove store = mock(Remove.class);
      when(store.getType()).thenReturn(Modification.Type.REMOVE);
      when(store.getKey()).thenReturn("1");
      when(cs.remove("1")).thenReturn(true);

      ModificationsTransactionWorker worker =
            new ModificationsTransactionWorker(cs,
                                               Collections.singletonList(store));
      worker.doWork();
   }

   @Test
   public void testDoWorkOnClear() throws Exception {
      CacheStore cs = mock(CacheStore.class);
      Clear clear = mock(Clear.class);
      when(clear.getType()).thenReturn(Modification.Type.CLEAR);
      cs.clear();

      ModificationsTransactionWorker worker =
            new ModificationsTransactionWorker(cs,
                                               Collections.singletonList(clear));
      worker.doWork();
   }

   @Test
   public void testDoWorkOnPurgeExpired() throws Exception {
      CacheStore cs = mock(CacheStore.class);
      PurgeExpired purge = mock(PurgeExpired.class);
      when(purge.getType()).thenReturn(Modification.Type.PURGE_EXPIRED);
      cs.purgeExpired();

      ModificationsTransactionWorker worker =
            new ModificationsTransactionWorker(cs,
                                               Collections.singletonList(purge));
      worker.doWork();
   }


//   case REMOVE:
//      Remove r = (Remove) modification;
//      cs.remove(r.getKey());
//      break;
//   default:
//      throw new IllegalArgumentException("Unknown modification type " + modification.getType());

}
