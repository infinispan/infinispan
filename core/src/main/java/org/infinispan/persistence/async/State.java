package org.infinispan.persistence.async;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import org.infinispan.commons.util.ByRef;
import org.infinispan.persistence.modifications.Clear;
import org.infinispan.persistence.modifications.Modification;
import org.infinispan.persistence.modifications.ModificationsList;
import org.infinispan.persistence.modifications.Remove;
import org.infinispan.persistence.modifications.Store;

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
    * Number of worker threads that currently work with this instance.
    */
   CountDownLatch workerThreads;

   public State(boolean clear, ConcurrentMap<Object, Modification> modMap, State next) {
      this.clear = clear;
      this.modifications = modMap;
      this.next = next;
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

   Map<Object, Modification> flattenModifications(ByRef<Boolean> containsClear) {
      Map<Object, Modification> map = new HashMap<>();
      for (State state = this; state != null; state = state.next) {
         // Make sure to add these before checking clear - as these are write operations done after the clear
         state.modifications.forEach(map::putIfAbsent);
         if (state.clear) {
            containsClear.set(Boolean.TRUE);
            break;
         }
      }
      return map;
   }

   /**
    * Adds the Modification(s) to the state map.
    *
    * @param mod
    *           the Modification to add, supports modification types STORE, REMOVE and LIST
    */
   void put(Modification mod) {
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
}
