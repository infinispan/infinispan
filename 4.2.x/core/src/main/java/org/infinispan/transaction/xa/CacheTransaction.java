package org.infinispan.transaction.xa;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.BidirectionalMap;

import java.util.List;

/**
 * Defines the state a infinispan transaction should have.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface CacheTransaction {

   /**
    * Returns the transaction identifier.
    */
   public GlobalTransaction getGlobalTransaction();

   /**
    * Returns the modifications visible within the current transaction.
    */
   public List<WriteCommand> getModifications();


   public CacheEntry lookupEntry(Object key);

   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries();

   public void putLookedUpEntry(Object key, CacheEntry e);

   public void removeLookedUpEntry(Object key);

   public void clearLookedUpEntries();
}
