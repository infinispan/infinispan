package org.infinispan.jmx;

import javax.management.MBeanServer;

/**
 * Implementors of this should return an MBeanServer to which MBeans will be registered.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.jmx.PlatformMBeanServerLookup
 * @since 4.0
 */
public interface MBeanServerLookup {

   public MBeanServer getMBeanServer();
}
