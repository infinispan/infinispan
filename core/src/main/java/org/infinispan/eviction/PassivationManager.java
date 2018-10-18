package org.infinispan.eviction;

import java.util.concurrent.CompletionStage;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.concurrent.CompletionStages;

import net.jcip.annotations.ThreadSafe;

/**
 * A passivation manager
 *
 * @author Manik Surtani
 * @version 4.1
 */
@ThreadSafe
@Scope(Scopes.NAMED_CACHE)
@MBean(objectName = "Passivation", description = "Component that handles passivating entries to a CacheStore on eviction.")
public interface PassivationManager extends JmxStatisticsExposer {

   boolean isEnabled();

   default void passivate(InternalCacheEntry entry) {
      CompletionStages.join(passivateAsync(entry));
   }

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
   @Stop(priority = 9)
   @ManagedOperation(
         description = "Passivate all entries to the CacheStore",
         displayName = "Passivate all")
   void passivateAll() throws PersistenceException;

   /**
    * Skips the passivation when the cache is stopped.
    */
   void skipPassivationOnStop(boolean skip);

   @ManagedAttribute(
         description = "Number of passivation events",
         displayName = "Number of cache passivations",
         measurementType = MeasurementType.TRENDSUP
   )
   long getPassivations();

   @ManagedOperation(
         description = "Resets statistics gathered by this component",
         displayName = "Reset statistics")
   void resetStatistics();
}
