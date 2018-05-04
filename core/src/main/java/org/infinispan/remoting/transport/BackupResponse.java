package org.infinispan.remoting.transport;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Represents a response from a backup replication call.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public interface BackupResponse {

   void waitForBackupToFinish() throws Exception;

   Map<String,Throwable> getFailedBackups();

   default Map<String, Object> getValues() {
      return Collections.emptyMap();
   };

   /**
    * Returns the list of sites where the backups failed due to a bridge communication error (as opposed to an
    * error caused by Infinispan, e.g. due to a lock acquisition timeout).
    */
   Set<String> getCommunicationErrors();

   /**
    * Return the time in millis when this operation was initiated.
    */
   long getSendTimeMillis();

   boolean isEmpty();
}
