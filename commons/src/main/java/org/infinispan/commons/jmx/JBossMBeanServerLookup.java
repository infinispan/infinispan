package org.infinispan.commons.jmx;

import java.util.Properties;

import javax.management.MBeanServer;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;

/**
 * MBeanServer lookup implementation to locate the JBoss MBeanServer.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public class JBossMBeanServerLookup implements MBeanServerLookup {

   @Override
   public MBeanServer getMBeanServer(Properties properties) {
      Class<?> mbsLocator = Util.loadClass("org.jboss.mx.util.MBeanServerLocator", null);
      try {
         return (MBeanServer) mbsLocator.getMethod("locateJBoss").invoke(null);
      } catch (Exception e) {
         throw new CacheException("Unable to locate JBoss MBean server", e);
      }
   }

}
