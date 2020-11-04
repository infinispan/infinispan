package org.infinispan.remoting.transport;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * Invokes a command in a remote site. This class allows to set a {@link org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.RetryPolicy}
 * to retry the command in case of an exception. The {@link org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.RetryPolicy}
 * has the exception to decide if it should retry or not.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class RetryOnFailureXSiteCommand<O> {

   public static final RetryPolicy NO_RETRY = new MaxRetriesPolicy(0);
   private static final Log log = LogFactory.getLog(RetryOnFailureXSiteCommand.class);
   private final boolean trace = log.isTraceEnabled();
   private final XSiteBackup xSiteBackup;
   private final XSiteReplicateCommand<O> command;
   private final RetryPolicy retryPolicy;

   private RetryOnFailureXSiteCommand(XSiteBackup backup, XSiteReplicateCommand<O> command, RetryPolicy retryPolicy) {
      this.xSiteBackup = backup;
      this.command = command;
      this.retryPolicy = retryPolicy;
   }

   /**
    * Invokes remotely the command using the {@code Transport} passed as parameter.
    *
    * @param rpcManager             the {@link RpcManager} to use.
    * @param waitTimeBetweenRetries the waiting time if the command fails before retrying it.
    * @param unit                   the {@link java.util.concurrent.TimeUnit} of the waiting time.
    * @throws Throwable if the maximum retries is reached (defined by the {@link org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.RetryPolicy},
    *                   the last exception occurred is thrown.
    */
   public void execute(RpcManager rpcManager, long waitTimeBetweenRetries, TimeUnit unit) throws Throwable {
      assertNotNull(rpcManager, "RpcManager");
      assertNotNull(unit, "TimeUnit");
      assertGreaterThanZero(waitTimeBetweenRetries, "WaitTimeBetweenRetries");

      do {
         try {
            CompletionStage<O> response = rpcManager.invokeXSite(xSiteBackup, command);
            response.toCompletableFuture().join();
            if (trace) {
               log.trace("Successful Response received.");
            }
            return;
         } catch (Throwable throwable) {
            throwable = CompletableFutures.extractException(throwable);
            if (!retryPolicy.retry(throwable, rpcManager)) {
               if (trace) {
                  log.tracef("Failing command with exception %s", throwable);
               }
               throw throwable;
            } else {
               if (trace) {
                  log.tracef("Will retry command after exception %s", throwable);
               }
            }
         }
         unit.sleep(waitTimeBetweenRetries);
      } while (true);
   }

   /**
    * It builds a new instance with the destination site, the command and the retry policy.
    *
    * @param backup      the destination site.
    * @param command     the command to invoke remotely.
    * @param retryPolicy the retry policy.
    * @return the new instance.
    * @throws java.lang.NullPointerException if any parameter is {@code null}
    */
   public static <O> RetryOnFailureXSiteCommand<O> newInstance(XSiteBackup backup, XSiteReplicateCommand<O> command,
                                                        RetryPolicy retryPolicy) {
      assertNotNull(backup, "XSiteBackup");
      assertNotNull(command, "XSiteReplicateCommand");
      assertNotNull(retryPolicy, "RetryPolicy");
      return new RetryOnFailureXSiteCommand<>(backup, command, retryPolicy);
   }

   @Override
   public String toString() {
      return "RetryOnLinkFailureXSiteCommand{" +
            "backup=" + xSiteBackup +
            ", command=" + command +
            '}';
   }

   private static void assertNotNull(Object value, String field) {
      if (value == null) {
         throw new NullPointerException(field + " must be not null.");
      }
   }

   private static void assertGreaterThanZero(long value, String field) {
      if (value <= 0) {
         throw new IllegalArgumentException(field + " must be greater that zero but instead it is " + value);
      }
   }

   public interface RetryPolicy {
      boolean retry(Throwable throwable, RpcManager transport);
   }

   public static class MaxRetriesPolicy implements RetryPolicy {

      private int maxRetries;

      public MaxRetriesPolicy(int maxRetries) {
         this.maxRetries = maxRetries;
      }

      @Override
      public boolean retry(Throwable throwable, RpcManager transport) {
         return maxRetries-- > 0;
      }
   }
}
