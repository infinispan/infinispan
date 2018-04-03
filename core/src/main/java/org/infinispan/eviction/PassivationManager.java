package org.infinispan.eviction;

import java.util.concurrent.CompletionStage;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.persistence.spi.PersistenceException;

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
    * Almost the same as {@link #passivateAsync(InternalCacheEntry)} except that it is performed
    * synchronously on the same thread that invoked it. This method will eventually be removed when
    * the data container can handle asynchronous passivation/activation.
    * @deprecated since 10.0 - please use {@link #passivateAsync(InternalCacheEntry)} instead.
    */
   void passivate(InternalCacheEntry entry);

   /**
    * This method will block the current thread while passivating the entry. This should be fixed when non blocking
    * cache stores are implemented. This method does not block while notifying listeners however.
    * @param entry entry to passivate
    * @return CompletionStage that when complete will have notified all listeners
    */
   CompletionStage<Void> passivateAsync(InternalCacheEntry entry);

   /**
    * Passivates all entries that are in memory. This method does not notify listeners of passivation.
    * @throws PersistenceException
    */
   void passivateAll() throws PersistenceException;

   /**
    * Skips the passivation when the cache is stopped.
    */
   void skipPassivationOnStop(boolean skip);

   long getPassivations();

   void resetStatistics();
}
