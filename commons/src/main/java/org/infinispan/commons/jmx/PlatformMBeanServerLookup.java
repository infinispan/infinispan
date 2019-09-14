package org.infinispan.commons.jmx;

import java.lang.management.ManagementFactory;
import java.util.Properties;

import javax.management.MBeanServer;

/**
 * Default implementation for {@link MBeanServerLookup}, will return the platform MBean server.
 * <p/>
 * Note: to enable platform MBeanServer the following system property should be passed to the Sun JVM:
 * <b>-Dcom.sun.management.jmxremote</b>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PlatformMBeanServerLookup implements MBeanServerLookup {

   @Override
   public MBeanServer getMBeanServer(Properties properties) {
      return ManagementFactory.getPlatformMBeanServer();
   }
}
