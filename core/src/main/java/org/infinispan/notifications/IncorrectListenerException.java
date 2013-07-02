package org.infinispan.notifications;

import org.infinispan.commons.CacheException;

/**
 * Thrown when an incorrectly annotated class is added as a cache listener using the {@link
 * org.infinispan.notifications.Listenable#addListener(Object)} API.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 4.0
 */
public class IncorrectListenerException extends CacheException {

   private static final long serialVersionUID = 3847404572671886703L;

   public IncorrectListenerException(String s) {
      super(s);
   }
}
