package org.infinispan.jmx;

import org.infinispan.commons.CacheConfigurationException;

/**
 * @author Mircea.Markus@jboss.com
 */
public class JmxDomainConflictException extends CacheConfigurationException {

   private static final long serialVersionUID = 8057798477119623578L;

   public JmxDomainConflictException(String msg) {
      super(msg);
   }
}
