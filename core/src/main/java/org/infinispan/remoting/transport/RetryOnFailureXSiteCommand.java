package org.infinispan.remoting.transport;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Invokes a command in a remote site. This class allows to set a {@link org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.RetryPolicy}
 * to retry the command in case of an exception. The {@link org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.RetryPolicy}
 * has the exception to decide if it should retry or not.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class RetryOnFailureXSiteCommand {

   public static final RetryPolicy NO_RETRY = new MaxRetriesPolicy(0);
   private static final Log log = LogFactory.getLog(RetryOnFailureXSiteCommand.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final boolean debug = log.isDebugEnabled();
   private final Collection<XSiteBackup> backups;
   private final XSiteReplicateCommand command;
   private final RetryPolicy retryPolicy;

   private RetryOnFailureXSiteCommand(Collection<XSiteBackup> backups, XSiteReplicateCommand command, RetryPolicy retryPolicy) {
      this.backups = backups;
      this.command = command;
      this.retryPolicy = retryPolicy;
   }

   /**
    * Invokes remotely the command using the {@code Transport} passed as parameter.
    *
    * @param transport              the {@link org.infinispan.remoting.transport.Transport} to use.
    * @param waitTimeBetweenRetries the waiting time if the command fails before retrying it.
    * @param unit                   the {@link java.util.concurrent.TimeUnit} of the waiting time.
    * @throws Throwable if the maximum retries is reached (defined by the {@link org.infinispan.remoting.transport.RetryOnFailureXSiteCommand.RetryPolicy},
    *                   the last exception occurred is thrown.
    */
   public void execute(Transport transport, long waitTimeBetweenRetries, TimeUnit unit) throws Throwable {
      if (backups.isEmpty()) {
         if (debug) {
            log.debugf("Executing '%s' but backup list is empty.", this);
         }
         return;
      }

      assertNotNull(transport, "Transport");
      assertNotNull(unit, "TimeUnit");
      assertGreaterThanZero(waitTimeBetweenRetries, "WaitTimeBetweenRetries");

      do {
         if (trace) {
            log.tracef("Sending %s to %s", command, backups);
         }
         BackupResponse response = transport.backupRemotely(backups, command);
         response.waitForBackupToFinish();
         Throwable throwable = extractThrowable(response);
         if (throwable == null) {
            if (trace) {
               log.trace("Successfull Response received.");
            }
            return;
         } else if (!retryPolicy.retry(throwable, transport)) {
            if (trace) {
               log.tracef("Exception Response received. Exception is %s", throwable);
            }
            throw throwable;
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
   public static RetryOnFailureXSiteCommand newInstance(XSiteBackup backup, XSiteReplicateCommand command,
                                                        RetryPolicy retryPolicy) {
      assertNotNull(backup, "XSiteBackup");
      assertNotNull(command, "XSiteReplicateCommand");
      assertNotNull(retryPolicy, "RetryPolicy");
      return new RetryOnFailureXSiteCommand(Collections.singletonList(backup), command, retryPolicy);
   }

   @Override
   public String toString() {
      return "RetryOnLinkFailureXSiteCommand{" +
            "backups=" + backups +
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

   private static Throwable extractThrowable(BackupResponse response) {
      Map<?, Throwable> errorMap = response.getFailedBackups();
      //noinspection ThrowableResultOfMethodCallIgnored
      return errorMap.isEmpty() ? null : errorMap.values().iterator().next();
   }

   public static interface RetryPolicy {
      boolean retry(Throwable throwable, Transport transport);
   }

   public static class MaxRetriesPolicy implements RetryPolicy {

      private int maxRetries;

      public MaxRetriesPolicy(int maxRetries) {
         this.maxRetries = maxRetries;
      }

      @Override
      public boolean retry(Throwable throwable, Transport transport) {
         return maxRetries-- > 0;
      }
   }
}
