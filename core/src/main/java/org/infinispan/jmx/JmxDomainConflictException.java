package org.infinispan.jmx;

import org.infinispan.CacheException;

/**
 * @author Mircea.Markus@jboss.com
 */
public class JmxDomainConflictException extends CacheException {
   public JmxDomainConflictException(String msg) {
      super(msg);
   }
}
