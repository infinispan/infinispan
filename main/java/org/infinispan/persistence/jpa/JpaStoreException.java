package org.infinispan.persistence.jpa;

import org.infinispan.persistence.spi.PersistenceException;

/**
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 *
 */
public class JpaStoreException extends PersistenceException {
   public JpaStoreException() {
      super();
   }

   public JpaStoreException(String message, Throwable cause) {
      super(message, cause);
   }

   public JpaStoreException(String message) {
      super(message);
   }

   public JpaStoreException(Throwable cause) {
      super(cause);
   }

}
