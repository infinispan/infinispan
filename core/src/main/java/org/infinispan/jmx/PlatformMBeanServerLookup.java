package org.infinispan.jmx;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;

/**
 * Default implementation for {@link MBeanServerLookup}, will return the platform MBean server.
 * <p/>
 * Note: to enable platform MBeanServer the following system property should be passet to the Sun JVM:
 * <b>-Dcom.sun.management.jmxremote</b>.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PlatformMBeanServerLookup implements MBeanServerLookup {

   public MBeanServer getMBeanServer() {
      return ManagementFactory.getPlatformMBeanServer();
   }
}
