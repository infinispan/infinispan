package org.infinispan.jcache.test.tck;

import com.sun.jmx.mbeanserver.JmxMBeanServer;

import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanServer;
import javax.management.MBeanServerBuilder;
import javax.management.MBeanServerDelegate;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;

/**
 * MBean Server builder instance for the TCK.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
public class TckMbeanServerBuilder extends MBeanServerBuilder {

   @Override
   public MBeanServer newMBeanServer(String defaultDomain, MBeanServer outer, MBeanServerDelegate delegate) {
      MBeanServerDelegate decorator = new TckMbeanServerDelegate(delegate);
      return JmxMBeanServer.newMBeanServer(defaultDomain, outer, decorator, false);
   }

   public class TckMbeanServerDelegate extends MBeanServerDelegate {
      private final MBeanServerDelegate delegate;

      public TckMbeanServerDelegate(MBeanServerDelegate delegate) {
         this.delegate = delegate;
      }

      @Override
      public synchronized String getMBeanServerId() {
         return System.getProperty("org.jsr107.tck.management.agentId");
      }

      @Override
      public String getSpecificationName() {
         return delegate.getSpecificationName();
      }

      @Override
      public String getSpecificationVersion() {
         return delegate.getSpecificationVersion();
      }

      @Override
      public String getSpecificationVendor() {
         return delegate.getSpecificationVendor();
      }

      @Override
      public String getImplementationName() {
         return delegate.getImplementationName();
      }

      @Override
      public String getImplementationVersion() {
         return delegate.getImplementationVersion();
      }

      @Override
      public String getImplementationVendor() {
         return delegate.getImplementationVendor();
      }

      @Override
      public MBeanNotificationInfo[] getNotificationInfo() {
         return delegate.getNotificationInfo();
      }

      @Override
      public synchronized void addNotificationListener(
            NotificationListener listener, NotificationFilter filter, Object handback) throws IllegalArgumentException {
         delegate.addNotificationListener(listener, filter, handback);
      }

      @Override
      public synchronized void removeNotificationListener(
            NotificationListener listener, NotificationFilter filter, Object handback) throws ListenerNotFoundException {
         delegate.removeNotificationListener(listener, filter, handback);
      }

      @Override
      public synchronized void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException {
         delegate.removeNotificationListener(listener);
      }

      @Override
      public void sendNotification(Notification notification) {
         delegate.sendNotification(notification);
      }
   }

}
