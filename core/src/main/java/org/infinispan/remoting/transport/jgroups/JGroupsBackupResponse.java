package org.infinispan.remoting.transport.jgroups;

import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.remoting.CacheUnreachableException;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.transport.BackupResponse;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.XSiteBackup;
import org.jgroups.UnreachableException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.*;
import static org.infinispan.commons.util.Util.formatString;
import static org.infinispan.commons.util.Util.prettyPrintTime;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class JGroupsBackupResponse implements BackupResponse {

   private static Log log = LogFactory.getLog(JGroupsBackupResponse.class);

   private final Map<XSiteBackup, Future<Object>> syncBackupCalls;
   private Map<String, Throwable> errors;
   private Set<String> communicationErrors;
   private final TimeService timeService;

   //there might be an significant difference in time between when the message is sent and when the actual wait
   // happens. Track that and adjust the timeouts accordingly.
   private long sendTimeNanos;

   public JGroupsBackupResponse(Map<XSiteBackup, Future<Object>> syncBackupCalls, TimeService timeService) {
      this.syncBackupCalls = syncBackupCalls;
      this.timeService = timeService;
      sendTimeNanos = timeService.time();
   }

   @Override
   public void waitForBackupToFinish() throws Exception {
      long deductFromTimeout = timeService.timeDuration(sendTimeNanos, MILLISECONDS);
      errors = new HashMap<String, Throwable>(syncBackupCalls.size());
      long elapsedTime = 0;
      for (Map.Entry<XSiteBackup, Future<Object>> entry : syncBackupCalls.entrySet()) {
                  
         long timeout = entry.getKey().getTimeout();
         String siteName = entry.getKey().getSiteName();

         if (timeout > 0) { //0 means wait forever
            timeout -= deductFromTimeout;
            timeout -= elapsedTime;
            if (timeout <= 0 && !entry.getValue().isDone() ) {
               log.tracef( "Timeout period %ld exhausted with site %s", entry.getKey().getTimeout(), siteName);
               errors.put(siteName, newTimeoutException(entry, entry.getKey().getTimeout()));
               addCommunicationError(siteName);
               continue;
            }
         }

         long startNanos = timeService.time();
         Object value = null;
         try {
            value = entry.getValue().get(timeout, MILLISECONDS);
         } catch (java.util.concurrent.TimeoutException te) {
            errors.put(siteName, newTimeoutException(entry, entry.getKey().getTimeout()));
            addCommunicationError(siteName);
         } catch (ExecutionException ue) {
            log.tracef(ue.getCause(), "Communication error with site %s", siteName);
            errors.put(siteName, filterException(ue.getCause()));
            addCommunicationError(siteName);
         } finally {
            elapsedTime += timeService.timeDuration(startNanos, MILLISECONDS);
         }

         if (value instanceof ExceptionResponse) {
            Exception remoteException = ((ExceptionResponse) value).getException();
            log.tracef(remoteException, "Got error backup response from site %s", siteName);
            errors.put(siteName, remoteException);
         } else {
            log.tracef("Received response from site %s: %s", siteName, value);
         }
      }
   }

   private void addCommunicationError(String siteName) {
      if (communicationErrors == null) //only create lazily as we don't expect communication errors to be the norm
         communicationErrors = new HashSet<String>(1);
      communicationErrors.add(siteName);
   }

   @Override
   public Set<String> getCommunicationErrors() {
      return communicationErrors == null ?
            InfinispanCollections.<String>emptySet() : communicationErrors;
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

   private TimeoutException newTimeoutException(Map.Entry<XSiteBackup, Future<Object>> entry, long timeout) {
      return new TimeoutException(formatString("Timed out after %s waiting for a response from %s",
                                               prettyPrintTime(timeout), entry.getKey()));
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
