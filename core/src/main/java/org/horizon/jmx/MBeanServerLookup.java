package org.horizon.jmx;

import javax.management.MBeanServer;

/**
 * Implementors of this should return an MBeanServer to which MBeans will be registered.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.horizon.jmx.PlatformMBeanServerLookup
 * @since 4.0
 */
public interface MBeanServerLookup {

   public MBeanServer getMBeanServer();
}
