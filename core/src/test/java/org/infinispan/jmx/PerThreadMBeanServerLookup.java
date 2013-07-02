package org.infinispan.jmx;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.util.Properties;

/**
 * Creates an MBeanServer on each thread.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PerThreadMBeanServerLookup implements MBeanServerLookup {

   static ThreadLocal<MBeanServer> threadMBeanServer = new ThreadLocal<MBeanServer>();

   public MBeanServer getMBeanServer(Properties properties) {
      return getThreadMBeanServer();
   }

   public static MBeanServer getThreadMBeanServer() {
      MBeanServer beanServer = threadMBeanServer.get();
      if (beanServer == null) {
         beanServer = MBeanServerFactory.createMBeanServer();
         threadMBeanServer.set(beanServer);
      }
      return beanServer;
   }
}
