package org.infinispan.transaction.xa;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.BidirectionalLinkedHashMap;
import org.infinispan.util.BidirectionalMap;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Defines the state of a remotely originated transaction.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class RemoteTransaction extends AbstractCacheTransaction implements Cloneable {

   private static Log log = LogFactory.getLog(RemoteTransaction.class);

   private volatile boolean valid = true;

   public RemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx) {
      this.modifications = modifications == null || modifications.length == 0 ? Collections.<WriteCommand>emptyList() : Arrays.asList(modifications);
      lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(this.modifications.size());
      this.tx = tx;
   }

   public RemoteTransaction(GlobalTransaction tx) {
      this.modifications = new LinkedList<WriteCommand>();
      lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>();
      this.tx = tx;
   }

   public void invalidate() {
      valid = false;
   }

   public void putLookedUpEntry(Object key, CacheEntry e) {
      if (valid) {
         if (log.isTraceEnabled()) {
            log.trace("Adding key " + key + " to tx " + getGlobalTransaction());
         }
         lookedUpEntries.put(key, e);
      } else {
         throw new InvalidTransactionException("This remote transaction " + getGlobalTransaction() + " is invalid");
      }
   }

   public void removeLookedUpEntry(Object key) {
      lookedUpEntries.remove(key);
   }

   public void clearLookedUpEntries() {
      lookedUpEntries.clear();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RemoteTransaction)) return false;
      RemoteTransaction that = (RemoteTransaction) o;
      return tx.equals(that.tx);
   }

   @Override
   public int hashCode() {
      return tx.hashCode();
   }

   @Override
   @SuppressWarnings("unchecked")
   public Object clone() {
      try {
         RemoteTransaction dolly = (RemoteTransaction) super.clone();
         dolly.modifications = new ArrayList<WriteCommand>(modifications);
         dolly.lookedUpEntries = lookedUpEntries.clone();
         return dolly;
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException("Impossible!!");
      }
   }

   @Override
   public String toString() {
      return "RemoteTransaction{" +
            "modifications=" + modifications +
            ", lookedUpEntries=" + lookedUpEntries +
            ", tx=" + tx +
            '}';
   }

   public Set<Object> getLockedKeys() {
      Set<Object> result = new HashSet<Object>();
      for (Object key : getLookedUpEntries().keySet()) {
         result.add(key);
      }
      if (lookedUpEntries.entrySet().size() != result.size())
         throw new IllegalStateException("Different sizes!");
      return result;
   }
}
