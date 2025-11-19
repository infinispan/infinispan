package org.infinispan.server.insights.scheduler;

import static com.redhat.insights.InsightsErrorCode.ERROR_SCHEDULED_SENT;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.server.insights.logging.Log;
import org.infinispan.util.concurrent.BlockingManager;

import com.redhat.insights.InsightsException;
import com.redhat.insights.InsightsScheduler;
import com.redhat.insights.config.InsightsConfiguration;
import com.redhat.insights.logging.InsightsLogger;

/**
 * Copy and adapted from {@link com.redhat.insights.InsightsCustomScheduledExecutor}.
 * Differently from the original scheduler,
 * instead of creating a new {@link java.util.concurrent.ScheduledExecutorService},
 * it delegates to {@link BlockingManager}.
 */
public class InfinispanInsightsScheduler implements InsightsScheduler {

   private static final Log log = Log.getLog(InfinispanInsightsScheduler.class);

   private final InsightsLogger logger;
   private final InsightsConfiguration configuration;
   private final BlockingManager blockingManager;

   private volatile boolean active = true;

   public InfinispanInsightsScheduler(InsightsLogger logger, InsightsConfiguration configuration,
                                      BlockingManager blockingManager) {
      this.logger = logger;
      this.configuration = configuration;
      this.blockingManager = blockingManager;
   }

   @Override
   public ScheduledFuture<?> scheduleConnect(Runnable sendConnect) {
      return scheduleAtFixedRate(sendConnect, 0, configuration.getConnectPeriod().getSeconds(), TimeUnit.SECONDS);
   }

   @Override
   public ScheduledFuture<?> scheduleJarUpdate(Runnable sendNewJarsIfAny) {
      return scheduleAtFixedRate(sendNewJarsIfAny,
            configuration.getUpdatePeriod().getSeconds(),
            configuration.getUpdatePeriod().getSeconds(),
            TimeUnit.SECONDS);
   }

   @Override
   public boolean isShutdown() {
      return false;
   }

   @Override
   public void shutdown() {
      active = false;
   }

   @Override
   public List<Runnable> shutdownNow() {
      return List.of();
   }

   public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
      if (!active) {
         log.clientSchedulerShutDown();
      }

      Runnable wrapped =
            () -> {
               try {
                  command.run();
               } catch (InsightsException ix) {
                  logger.error(ERROR_SCHEDULED_SENT.formatMessage(
                        "Red Hat Insights client scheduler shutdown, scheduled send failed: " + ix.getMessage()), ix);
                  shutdown();
                  throw ix;
               } catch (Exception x) {
                  logger.error(ERROR_SCHEDULED_SENT.formatMessage(
                        "Red Hat Insights client scheduler shutdown, non-Insights failure: " + x.getMessage()), x);
                  shutdown();
                  throw x;
               }
            };

      return blockingManager.scheduleRunBlockingAtFixedRate(wrapped, initialDelay, period, unit, this);
   }
}
