package org.infinispan.remoting.transport.jgroups;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.commons.util.Util.formatString;
import static org.infinispan.commons.util.Util.prettyPrintTime;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.infinispan.remoting.CacheUnreachableException;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.jgroups.UnreachableException;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class JGroupsBackupResponse implements BackupResponse {

   private static Log log = LogFactory.getLog(JGroupsBackupResponse.class);

   private final Map<XSiteBackup, Future<Response>> syncBackupCalls;
   private Map<String, Throwable> errors;
   private Set<String> communicationErrors;
   private final TimeService timeService;

   //there might be an significant difference in time between when the message is sent and when the actual wait
   // happens. Track that and adjust the timeouts accordingly.
   private long sendTimeNanos;

   public JGroupsBackupResponse(Map<XSiteBackup, Future<Response>> syncBackupCalls, TimeService timeService) {
      this.syncBackupCalls = syncBackupCalls;
      this.timeService = timeService;
      sendTimeNanos = timeService.time();
   }

   @Override
   public void waitForBackupToFinish() throws Exception {
      long deductFromTimeout = timeService.timeDuration(sendTimeNanos, MILLISECONDS);
      errors = new HashMap<>(syncBackupCalls.size());
      long elapsedTime = 0;
      for (Map.Entry<XSiteBackup, Future<Response>> entry : syncBackupCalls.entrySet()) {

         XSiteBackup xSiteBackup = entry.getKey();
         long timeout = xSiteBackup.getTimeout();
         String siteName = xSiteBackup.getSiteName();

         if (timeout > 0) { //0 means wait forever
            timeout -= deductFromTimeout;
            timeout -= elapsedTime;
            if (timeout <= 0 && !entry.getValue().isDone() ) {
               log.tracef("Timeout period %ld exhausted with site %s", xSiteBackup.getTimeout(), siteName);
               errors.put(siteName, newTimeoutException(xSiteBackup.getTimeout(), xSiteBackup));
               addCommunicationError(siteName);
               continue;
            }
         }

         long startNanos = timeService.time();
         Response response = null;
         try {
            response = entry.getValue().get(timeout, MILLISECONDS);
         } catch (java.util.concurrent.TimeoutException te) {
            errors.put(siteName, newTimeoutException(xSiteBackup.getTimeout(), xSiteBackup));
            addCommunicationError(siteName);
         } catch (ExecutionException ue) {
            Throwable cause = ue.getCause();
            cause.addSuppressed(ue);
            log.tracef(cause, "Communication error with site %s", siteName);
            errors.put(siteName, filterException(cause));
            addCommunicationError(siteName);
         } finally {
            elapsedTime += timeService.timeDuration(startNanos, MILLISECONDS);
         }

         if (response instanceof ExceptionResponse) {
            Throwable remoteException = ((ExceptionResponse) response).getException();
            log.tracef(remoteException, "Got error backup response from site %s", siteName);
            errors.put(siteName, remoteException);
         } else {
            log.tracef("Received response from site %s: %s", siteName, response);
         }
      }
   }

   private void addCommunicationError(String siteName) {
      if (communicationErrors == null) //only create lazily as we don't expect communication errors to be the norm
         communicationErrors = new HashSet<>(1);
      communicationErrors.add(siteName);
   }

   @Override
   public Set<String> getCommunicationErrors() {
      return communicationErrors == null ?
            Collections.emptySet() : communicationErrors;
   }

   @Override
   public long getSendTimeMillis() {
      return NANOSECONDS.toMillis(sendTimeNanos);
   }

   @Override
   public boolean isEmpty() {
      return syncBackupCalls == null || syncBackupCalls.isEmpty();
   }

   @Override
   public Map<String, Throwable> getFailedBackups() {
      return errors;
   }

   private TimeoutException newTimeoutException(long timeout, XSiteBackup xSiteBackup) {
      return new TimeoutException(formatString("Timed out after %s waiting for a response from %s",
                                               prettyPrintTime(timeout), xSiteBackup));
   }

   @Override
   public String toString() {
      return "JGroupsBackupResponse{" +
            "syncBackupCalls=" + syncBackupCalls +
            ", errors=" + errors +
            ", communicationErrors=" + communicationErrors +
            ", sendTimeNanos=" + sendTimeNanos +
            '}';
   }

   private Throwable filterException(Throwable throwable) {
      if (throwable instanceof UnreachableException) {
         return new CacheUnreachableException((UnreachableException) throwable);
      }
      return throwable;
   }
}
