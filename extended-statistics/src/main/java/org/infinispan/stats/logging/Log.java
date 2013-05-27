package org.infinispan.stats.logging;

import org.infinispan.stats.container.ExtendedStatistic;
import org.infinispan.stats.percentiles.PercentileStatistic;
import org.jboss.logging.Cause;
import org.jboss.logging.LogMessage;
import org.jboss.logging.Message;
import org.jboss.logging.MessageLogger;

import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

/**
 * The JBoss Logging interface which defined the logging methods for the extended statistics module. The id range for
 * this is 25001-26000
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = WARN)
   @Message(id = 25001, value = "Extended Statistic [%s] not found while tried to add a percentile sample.")
   void extendedStatisticNotFoundForPercentile(ExtendedStatistic statistic, @Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(id = 25002, value = "Trying to mark the transaction [%s] as write transaction but no transaction is associated.")
   void markUnexistingTransactionAsWriteTransaction(String transaction);

   @LogMessage(level = WARN)
   @Message(id = 25003, value = "Trying to prepare transaction [%s] but no transaction is associated.")
   void prepareOnUnexistingTransaction(String transaction);

   @LogMessage(level = WARN)
   @Message(id = 25004, value = "Trying to set the transaction [%s] outcome to %s but no transaction is associated.")
   void outcomeOnUnexistingTransaction(String transaction, String outcome);

   @LogMessage(level = WARN)
   @Message(id = 25005, value = "Unable calculate local execution time without contention.")
   void unableToCalculateLocalExecutionTimeWithoutContention(@Cause Throwable throwable);

   @LogMessage(level = WARN)
   @Message(id = 25006, value = "Unable to copy value from %s to %s.")
   void unableToCopyValue(ExtendedStatistic from, ExtendedStatistic to, @Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(id = 25007, value = "Unable to get extended statistic %s.")
   void unableToGetStatistic(ExtendedStatistic statistic, @Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(id = 25008, value = "Unable to get %s-th percentile for %s.")
   void unableToGetPercentile(int percentile, PercentileStatistic statistic, @Cause Throwable cause);

   @LogMessage(level = INFO)
   @Message(id = 25009, value = "Replacing original components.")
   void replaceComponents();

   @LogMessage(level = INFO)
   @Message(id = 25010, value = "Replacing %s. old=[%s] new=[%s].")
   void replaceComponent(String componentName, Object oldComponent, Object newComponent);

   @LogMessage(level = INFO)
   @Message(id = 25011, value = "Starting ExtendedStatisticInterceptor.")
   void startExtendedStatisticInterceptor();

   @LogMessage(level = INFO)
   @Message(id = 25012, value = "Starting CacheUsageInterceptor.")
   void startStreamSummaryInterceptor();

   @LogMessage(level = INFO)
   @Message(id = 25013, value = "Stopping CacheUsageInterceptor.")
   void stopStreamSummaryInterceptor();

}
