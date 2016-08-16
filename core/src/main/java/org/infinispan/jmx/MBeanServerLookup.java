package org.infinispan.jmx;

import java.util.Properties;

import javax.management.MBeanServer;

/**
 * Implementors of this should return an MBeanServer to which MBeans will be registered.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.jmx.PlatformMBeanServerLookup
 * @since 4.0
 */
public interface MBeanServerLookup {
   /**
    * Retrieves an {@link MBeanServer} instance.
    * @param properties properties to configure the MBeanServer instance
    * @return an MBeanServer instance
    */
   MBeanServer getMBeanServer(Properties properties);
}
