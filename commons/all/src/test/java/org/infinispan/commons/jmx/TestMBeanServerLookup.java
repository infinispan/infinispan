package org.infinispan.commons.jmx;

import java.util.Properties;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

/**
 * A lazy implementations of MBeanServerLookup. The MBeanServer is created only on first call to {@link
 * MBeanServerLookup#getMBeanServer}. The MBeanServer is cached and the same instance is returned on subsequent calls.
 *
 * @author anistor@redhat.com
 * @since 10.0
 */
public final class TestMBeanServerLookup implements MBeanServerLookup {

   private MBeanServer mBeanServer;

   /**
    * Factory method
    */
   public static MBeanServerLookup create() {
      return new TestMBeanServerLookup();
   }

   @Override
   public synchronized MBeanServer getMBeanServer(Properties properties) {
      if (mBeanServer == null) {
         mBeanServer = MBeanServerFactory.newMBeanServer();
      }
      return mBeanServer;
   }
}
