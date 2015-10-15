package org.infinispan.eviction;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.persistence.spi.PersistenceException;

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

   void passivate(InternalCacheEntry entry);

   void passivateAll() throws PersistenceException;

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
