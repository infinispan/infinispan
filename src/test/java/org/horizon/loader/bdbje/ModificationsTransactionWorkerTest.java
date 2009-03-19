package org.horizon.loader.bdbje;

import static org.easymock.classextension.EasyMock.*;
import org.horizon.loader.CacheStore;
import org.horizon.loader.StoredEntry;
import org.horizon.loader.modifications.Clear;
import org.horizon.loader.modifications.Modification;
import org.horizon.loader.modifications.PurgeExpired;
import org.horizon.loader.modifications.Remove;
import org.horizon.loader.modifications.Store;
import org.testng.annotations.Test;

import java.util.Collections;

/**
 * Unit tests that cover {@link  ModificationsTransactionWorker }
 *
 * @author Adrian Cole
 * @version $Id: $
 * @since 1.0
 */
@Test(groups = "unit", enabled = true, testName = "loader.bdbje.ModificationsTransactionWorkerTest")
public class ModificationsTransactionWorkerTest {

   @Test
   public void testDoWorkOnUnsupportedModification() {
      //TODO: we currently support all modifications...
   }

   @Test
   public void testDoWorkOnStore() throws Exception {
      CacheStore cs = createMock(CacheStore.class);
      Store store = createMock(Store.class);
      StoredEntry entry = new StoredEntry("1", "2");
      expect(store.getType()).andReturn(Modification.Type.STORE);
      expect(store.getStoredEntry()).andReturn(entry);
      cs.store(entry);
      replay(cs);
      replay(store);

      ModificationsTransactionWorker worker =
            new ModificationsTransactionWorker(cs,
                                               Collections.singletonList(store));
      worker.doWork();
      verify(cs);
      verify(store);

   }

   @Test
   public void testDoWorkOnRemove() throws Exception {
      CacheStore cs = createMock(CacheStore.class);
      Remove store = createMock(Remove.class);
      expect(store.getType()).andReturn(Modification.Type.REMOVE);
      expect(store.getKey()).andReturn("1");
      expect(cs.remove("1")).andReturn(true);
      replay(cs);
      replay(store);

      ModificationsTransactionWorker worker =
            new ModificationsTransactionWorker(cs,
                                               Collections.singletonList(store));
      worker.doWork();
      verify(cs);
      verify(store);

   }

   @Test
   public void testDoWorkOnClear() throws Exception {
      CacheStore cs = createMock(CacheStore.class);
      Clear clear = createMock(Clear.class);
      expect(clear.getType()).andReturn(Modification.Type.CLEAR);
      cs.clear();
      replay(cs);
      replay(clear);

      ModificationsTransactionWorker worker =
            new ModificationsTransactionWorker(cs,
                                               Collections.singletonList(clear));
      worker.doWork();
      verify(cs);
      verify(clear);
   }

   @Test
   public void testDoWorkOnPurgeExpired() throws Exception {
      CacheStore cs = createMock(CacheStore.class);
      PurgeExpired purge = createMock(PurgeExpired.class);
      expect(purge.getType()).andReturn(Modification.Type.PURGE_EXPIRED);
      cs.purgeExpired();
      replay(cs);
      replay(purge);

      ModificationsTransactionWorker worker =
            new ModificationsTransactionWorker(cs,
                                               Collections.singletonList(purge));
      worker.doWork();
      verify(cs);
      verify(purge);
   }


//   case REMOVE:
//      Remove r = (Remove) modification;
//      cs.remove(r.getKey());
//      break;
//   default:
//      throw new IllegalArgumentException("Unknown modification type " + modification.getType());

}
