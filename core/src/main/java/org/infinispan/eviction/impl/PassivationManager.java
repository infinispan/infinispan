package org.infinispan.eviction.impl;

import java.util.concurrent.CompletionStage;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.JmxStatisticsExposer;

/**
 * A passivation manager
 *
 * @author Manik Surtani
 * @version 4.1
 */
@ThreadSafe
@Scope(Scopes.NAMED_CACHE)
public interface PassivationManager extends JmxStatisticsExposer {

   boolean isEnabled();

   /**
    * Passivates the entry in a non blocking fashion.
    * @param entry entry to passivate
    * @return CompletionStage that when complete will have passivated the entry and notified listeners
    */
   CompletionStage<Void> passivateAsync(InternalCacheEntry<?, ?> entry);

   /**
    * Start passivating all entries that are in memory.
    *
    * This method does not notify listeners of passivation.
    *
    * @since 10.1
    */
   CompletionStage<Void> passivateAllAsync();

   /**
    * Skips the passivation when the cache is stopped.
    */
   void skipPassivationOnStop(boolean skip);

   long getPassivations();

   void resetStatistics();
}
