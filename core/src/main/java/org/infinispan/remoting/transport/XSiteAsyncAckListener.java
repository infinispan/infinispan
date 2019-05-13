package org.infinispan.remoting.transport;

/**
 * A listener to be notified when an asynchronous cross-site request is completed.
 *
 * @author Pedro Ruivo
 * @since 10.0
 */
@FunctionalInterface
public interface XSiteAsyncAckListener {

   /**
    * Invoked when an ack for an asynchronous request is received.
    * <p>
    * If an exception is received (could be a network exception or an exception from the remote site), the {@code
    * throwable} is set to a non {@code null} value.
    *
    * @param sendTimestamp The timestamp when the request was sent to the remote site (nanoseconds).
    * @param siteName      The remote site name.
    * @param throwable     The exception received (including timeouts and site unreachable) or {@code null}.
    */
   void onAckReceived(long sendTimestamp, String siteName, Throwable throwable);

}
