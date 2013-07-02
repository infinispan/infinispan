package org.infinispan.jmx;

import org.infinispan.commons.CacheException;

/**
 * @author Mircea.Markus@jboss.com
 */
public class JmxDomainConflictException extends CacheException {

   private static final long serialVersionUID = 8057798477119623578L;

   public JmxDomainConflictException(String msg) {
      super(msg);
   }
}
