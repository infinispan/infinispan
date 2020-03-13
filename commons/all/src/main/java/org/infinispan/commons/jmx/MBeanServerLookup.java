package org.infinispan.commons.jmx;

import java.util.Properties;

import javax.management.MBeanServer;

/**
 * Implementors of this should return an MBeanServer to which MBeans will be registered.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.commons.jmx.PlatformMBeanServerLookup
 * @since 4.0
 */
public interface MBeanServerLookup {

   /**
    * Retrieves an {@link MBeanServer} instance.
    *
    * @param properties optional properties (can be null) to configure the MBeanServer instance
    * @return an MBeanServer instance
    */
   MBeanServer getMBeanServer(Properties properties);

   default MBeanServer getMBeanServer() {
      return getMBeanServer(null);
   }
}
