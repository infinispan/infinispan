package org.infinispan.jmx;

import java.util.Properties;

import javax.management.MBeanServer;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;

/**
 * MBeanServer lookup implementation to locate the JBoss MBeanServer.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public class JBossMBeanServerLookup implements MBeanServerLookup {
	
	private GlobalConfiguration globalConfiguration;
	
	@Inject
	public void init(GlobalConfiguration globalConfiguration) {
		this.globalConfiguration = globalConfiguration;
	}

   @Override
   public MBeanServer getMBeanServer(Properties properties) {
      try {
          Class<?> mbsLocator = globalConfiguration.classLoader().loadClass(
        		  "org.jboss.mx.util.MBeanServerLocator");
         return (MBeanServer) mbsLocator.getMethod("locateJBoss").invoke(null);
      } catch (Exception e) {
         throw new CacheException("Unable to locate JBoss MBean server", e);
      }
   }

}
