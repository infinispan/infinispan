package org.infinispan.transaction.xa;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.util.BidirectionalMap;

import java.util.List;
import java.util.Map;

/**
 * // TODO: Mircea: Document this!
 *
 * @author
 */
public interface CacheTransaction {

   public GlobalTransaction getGlobalTransaction();

   public List<WriteCommand> getModifications();

   public CacheEntry lookupEntry(Object key);

   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries();

   public void putLookedUpEntry(Object key, CacheEntry e);

   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries);

   public void removeLookedUpEntry(Object key);

   public void clearLookedUpEntries();
}
