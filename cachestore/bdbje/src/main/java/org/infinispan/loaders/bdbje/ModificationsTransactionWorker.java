package org.infinispan.loaders.bdbje;

import com.sleepycat.collections.TransactionWorker;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;

import java.util.List;

/**
 * Adapter that allows a list of {@link Modification}s to be performed atomically via {@link
 * com.sleepycat.collections.TransactionRunner}.
 *
 * @author Adrian Cole
 * @since 4.0
 */
public class ModificationsTransactionWorker implements TransactionWorker {
   private List<? extends Modification> mods;
   private CacheStore cs;

   /**
    * Associates {@link Modification}s that will be applied to the supplied {@link CacheStore}
    *
    * @param store what to affect
    * @param mods  actions to take
    */
   public ModificationsTransactionWorker(CacheStore store, List<? extends Modification> mods) {
      this.cs = store;
      this.mods = mods;
   }

   /**
    * {@inheritDoc} This implementation iterates through a list of work represented by {@link Modification} objects and
    * executes it against the {@link CacheStore}.<p/> Current commands supported are: <ul> <li>STORE</li> <li>CLEAR</li>
    * <li>REMOVE</li> <li>PURGE_EXPIRED</li> </ul>
    */
   @Override
   public void doWork() throws Exception {
      for (Modification modification : mods)
         switch (modification.getType()) {
            case STORE:
               Store s = (Store) modification;
               cs.store(s.getStoredEntry());
               break;
            case CLEAR:
               cs.clear();
               break;
            case REMOVE:
               Remove r = (Remove) modification;
               cs.remove(r.getKey());
               break;
            case PURGE_EXPIRED:
               cs.purgeExpired();
               break;
            default:
               throw new IllegalArgumentException("Unknown modification type " + modification.getType());
         }
   }
}
