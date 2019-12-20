package org.infinispan.eviction.impl;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
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
public abstract class AbstractPassivationManager implements PassivationManager {
   /**
    * Passivates all entries that are in memory. This method does not notify listeners of passivation.
    * @throws PersistenceException
    */
   @Stop(priority = 9)
   @ManagedOperation(
         description = "Passivate all entries to the CacheStore",
         displayName = "Passivate all")
   public abstract void passivateAll() throws PersistenceException;

   @ManagedAttribute(
         description = "Number of passivation events",
         displayName = "Number of cache passivations",
         measurementType = MeasurementType.TRENDSUP
   )
   public abstract long getPassivations();

   @ManagedOperation(
         description = "Resets statistics gathered by this component",
         displayName = "Reset statistics")
   public abstract void resetStatistics();
}
