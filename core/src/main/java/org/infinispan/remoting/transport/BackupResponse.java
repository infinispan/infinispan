package org.infinispan.remoting.transport;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Represents a response from a backup replication call.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public interface BackupResponse {

   void waitForBackupToFinish() throws Exception;

   Map<String,Throwable> getFailedBackups();

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

   /**
    * Registers a listener that is notified when the cross-site request is finished.
    * <p>
    * The parameter is the time spent in the network in milliseconds.
    *
    * @param timeElapsedConsumer The {@link Consumer} to be invoke.
    */
   void notifyFinish(LongConsumer timeElapsedConsumer);

   /**
    * Invokes {@link XSiteAsyncAckListener} for each ack received from an asynchronous cross site request.
    *
    * If the request times-out or failed to be sent, the listeners receives a non-null {@link Throwable}.
    */
   void notifyAsyncAck(XSiteAsyncAckListener listener);

   /**
    * @return {@code true} if the request for the remote site is synchronous.
    */
   boolean isSync(String siteName);
}
