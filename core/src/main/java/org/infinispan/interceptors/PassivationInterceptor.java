package org.infinispan.interceptors;

import org.infinispan.commands.write.EvictCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.JmxStatsCommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
/**
 * Writes evicted entries back to the store on the way in through the CacheStore
 *
 * @since 4.0
 */
@MBean(objectName = "Passivation", description = "Component that handles passivating entries to a CacheStore on eviction.")
public class PassivationInterceptor extends JmxStatsCommandInterceptor {


   PassivationManager passivator;
   DataContainer dataContainer;

   private static final Log log = LogFactory.getLog(PassivationInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void setDependencies(PassivationManager passivator, DataContainer dataContainer) {
      this.passivator = passivator;
      this.dataContainer = dataContainer;
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      Object key = command.getKey();
      passivator.passivate(dataContainer.get(key));
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   @ManagedOperation(
         description = "Resets statistics gathered by this component", displayName = "Reset statistics")
   public void resetStatistics() {
      passivator.resetPassivationCount();
   }

   @ManagedAttribute(
         description = "Number of passivation events",
         displayName = "Number of cache passivations",
         measurementType = MeasurementType.TRENDSUP
   )
   public String getPassivations() {
      if (!getStatisticsEnabled()) return "N/A";
      return String.valueOf(passivator.getPassivationCount());
   }
}
