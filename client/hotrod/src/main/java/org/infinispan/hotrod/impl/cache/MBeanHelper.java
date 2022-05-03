package org.infinispan.hotrod.impl.cache;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.hotrod.configuration.StatisticsConfiguration;
import org.infinispan.hotrod.impl.HotRodTransport;

/**
 * @since 14.0
 **/
public interface MBeanHelper extends AutoCloseable {
   void register(RemoteCacheImpl<?, ?> remoteCache);

   void unregister(RemoteCacheImpl<?, ?> remoteCache);

   void close();

   static MBeanHelper getInstance(HotRodTransport transport) {
      if (transport.getConfiguration().statistics().jmxEnabled()) {
         return new Impl(transport);
      } else {
         return new NoOp();
      }
   }

   class Impl implements MBeanHelper {
      private final MBeanServer mBeanServer;
      private final ObjectName transportMBean;
      private final Map<String, ObjectName> cacheMBeans = new HashMap<>();

      Impl(HotRodTransport transport) {
         StatisticsConfiguration configuration = transport.getConfiguration().statistics();
         try {
            mBeanServer = configuration.mbeanServerLookup().getMBeanServer();
            transportMBean = new ObjectName(String.format("%s:type=HotRodClient,name=%s", configuration.jmxDomain(), configuration.jmxName()));
            mBeanServer.registerMBean(this, transportMBean);
         } catch (Exception e) {
            throw HOTROD.jmxRegistrationFailure(e);
         }
      }

      @Override
      public void close() {
         unregister(transportMBean);
      }

      public void register(RemoteCacheImpl<?, ?> remoteCache) {
         StatisticsConfiguration configuration = remoteCache.getHotRodTransport().getConfiguration().statistics();
         try {
            ObjectName mbeanObjectName = new ObjectName(String.format("%s:type=HotRodClient,name=%s,cache=%s", transportMBean.getDomain(), configuration.jmxName(), remoteCache.getName()));
            mBeanServer.registerMBean(remoteCache.getClientStatistics(), mbeanObjectName);
            cacheMBeans.put(remoteCache.getName(), mbeanObjectName);
         } catch (Exception e) {
            throw HOTROD.jmxRegistrationFailure(e);
         }
      }

      public void unregister(RemoteCacheImpl<?, ?> remoteCache) {
         unregister(cacheMBeans.remove(remoteCache.getName()));
      }

      private void unregister(ObjectName objectName) {
         if (objectName != null) {
            try {
               if (mBeanServer.isRegistered(objectName)) {
                  mBeanServer.unregisterMBean(objectName);
               } else {
                  HOTROD.debugf("MBean not registered: %s", objectName);
               }
            } catch (Exception e) {
               throw HOTROD.jmxUnregistrationFailure(e);
            }
         }
      }
   }

   class NoOp implements MBeanHelper {
      @Override
      public void close() {
      }

      @Override
      public void register(RemoteCacheImpl<?, ?> remoteCache) {
      }

      @Override
      public void unregister(RemoteCacheImpl<?, ?> remoteCache) {
      }
   }
}
