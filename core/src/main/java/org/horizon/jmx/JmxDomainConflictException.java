package org.horizon.jmx;

import org.horizon.CacheException;

/**
 * @author Mircea.Markus@jboss.com
 */
public class JmxDomainConflictException extends CacheException {
   public JmxDomainConflictException(String msg) {
      super(msg);
   }
}
