package org.infinispan.client.hotrod.exceptions;

/**
 * When a remote node is suspected and evicted from the cluster while an
 * operation is ongoing, the Hot Rod client emits this exception.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public class RemoteNodeSuspectException extends HotRodClientException {

   public RemoteNodeSuspectException(String msgFromServer, long messageId, short status) {
      super(msgFromServer, messageId, status);
   }

}
