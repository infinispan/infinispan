package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;

/**
 * @author Galder Zamarreño
 */
public class IncorrectClientListenerException extends HotRodClientException {
   public IncorrectClientListenerException(String message) {
      super(message);
   }
}
