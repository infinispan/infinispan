package org.infinispan.persistence.async;

import org.infinispan.commons.CacheException;
import org.infinispan.persistence.modifications.Clear;
import org.infinispan.persistence.modifications.Modification;
import org.infinispan.persistence.modifications.ModificationsList;
import org.infinispan.persistence.modifications.Remove;
import org.infinispan.persistence.modifications.Store;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

/**
* @author Karsten Blees
* @since 6.0
*/
public class State {

   final static Clear CLEAR = new Clear();

   /**
    * True if the state has been cleared before making modifications.
    */
   final boolean clear;

   /**
    * Modifications to apply to the back-end CacheStore.
    */
   final ConcurrentMap<Object, Modification> modifications;

   /**
    * Next state in the chain, initialized in constructor, may be set to <code>null</code>
    * asynchronously at any time.
    */
   volatile State next;

   /**
    * True if the CacheStore has been stopped (i.e. this is the last state to process).
    */
   volatile boolean stopped = false;

   /**
    * Number of worker threads that currently work with this instance.
    */
   CountDownLatch workerThreads;

   State(boolean clear, ConcurrentMap<Object, Modification> modMap, State next) {
      this.clear = clear;
      this.modifications = modMap;
      this.next = next;
      if (next != null)
         stopped = next.stopped;
   }

   /**
    * Gets the Modification for the specified key from this State object or chained (
    * <code>next</code>) State objects.
    *
    * @param key
    *           the key to look up
    * @return the Modification for the specified key, or <code>CLEAR</code> if the state was
    *         cleared, or <code>null</code> if the key is not in the state map
    */
   Modification get(Object key) {
      for (State state = this; state != null; state = state.next) {
         Modification mod = state.modifications.get(key);
         if (mod != null)
            return mod;
         else if (state.clear)
            return CLEAR;
      }
      return null;
   }

   /**
    * Adds the Modification(s) to the state map.
    *
    * @param mod
    *           the Modification to add, supports modification types STORE, REMOVE and LIST
    */
   void put(Modification mod) {
      if (stopped)
         throw new CacheException("AsyncCacheWriter stopped; no longer accepting more entries.");
      switch (mod.getType()) {
         case STORE:
            modifications.put(((Store) mod).getKey(), mod);
            break;
         case REMOVE:
            modifications.put(((Remove) mod).getKey(), mod);
            break;
         case LIST:
            for (Modification m : ((ModificationsList) mod).getList())
               put(m);
            break;
         default:
            throw new IllegalArgumentException("Unknown modification type " + mod.getType());
      }
   }


   public Set getKeysInTransit() {
      Set result = new HashSet();
      _loadKeys(this, result);
      return result;
   }

   private void _loadKeys(State s, Set result) {
      // if not cleared, get keys from next State or the back-end store
      if (!s.clear) {
         State next = s.next;
         if (next != null)
            _loadKeys(next, result);
      }

      // merge keys of the current State
      for (Modification mod : s.modifications.values()) {
         switch (mod.getType()) {
            case STORE:
               Object key = ((Store) mod).getKey();
                  result.add(key);
               break;
            case REMOVE:
               result.remove(((Remove) mod).getKey());
               break;
         }
      }
   }

}
