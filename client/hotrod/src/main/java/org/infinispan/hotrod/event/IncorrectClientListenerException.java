package org.infinispan.hotrod.event;

import org.infinispan.hotrod.exceptions.HotRodClientException;

/**
 */
public class IncorrectClientListenerException extends HotRodClientException {
   public IncorrectClientListenerException(String message) {
      super(message);
   }
}
