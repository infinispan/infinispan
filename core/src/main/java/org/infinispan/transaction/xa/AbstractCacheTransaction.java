package org.infinispan.transaction.xa;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.BidirectionalLinkedHashMap;
import org.infinispan.util.BidirectionalMap;

import java.util.Arrays;
import java.util.List;

/**
 * Base class for local and remote transaction.
 * Impl note: The aggregated modification list and lookedUpEntries are not instantiated here but in subclasses.
 * This is done in order to take advantage of the fact that, for remote transactions we already know the size of the
 * modifications list at creation time.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public abstract class AbstractCacheTransaction implements CacheTransaction {

   protected List<WriteCommand> modifications;
   protected BidirectionalLinkedHashMap<Object, CacheEntry> lookedUpEntries;
   protected GlobalTransaction tx;


   public GlobalTransaction getGlobalTransaction() {
      return tx;
   }

   public List<WriteCommand> getModifications() {
      return modifications;
   }

   public void setModifications(WriteCommand[] modifications) {
      this.modifications = Arrays.asList(modifications);
   }

   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      return lookedUpEntries;
   }

   public CacheEntry lookupEntry(Object key) {
      if (lookedUpEntries == null) return null;
      return lookedUpEntries.get(key);
   }

   public void removeLookedUpEntry(Object key) {
      if (lookedUpEntries != null) lookedUpEntries.remove(key);
   }

   public void clearLookedUpEntries() {
      if (lookedUpEntries != null) lookedUpEntries.clear();
   }
}
