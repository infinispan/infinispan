package org.infinispan.jcache.tck;

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
public final class TckMbeanServerBuilder extends MBeanServerBuilder {

   @Override
   public MBeanServer newMBeanServer(String defaultDomain, MBeanServer outer, MBeanServerDelegate delegate) {
      return new MBeanServerBuilder().newMBeanServer(defaultDomain, outer, new TckMBeanServerDelegate(delegate));
   }

   private static final class TckMBeanServerDelegate extends MBeanServerDelegate {

      private final MBeanServerDelegate delegate;

      TckMBeanServerDelegate(MBeanServerDelegate delegate) {
         this.delegate = delegate;
      }

      @Override
      public String getMBeanServerId() {
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
      public void addNotificationListener(
            NotificationListener listener, NotificationFilter filter, Object handback) throws IllegalArgumentException {
         delegate.addNotificationListener(listener, filter, handback);
      }

      @Override
      public void removeNotificationListener(
            NotificationListener listener, NotificationFilter filter, Object handback) throws ListenerNotFoundException {
         delegate.removeNotificationListener(listener, filter, handback);
      }

      @Override
      public void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException {
         delegate.removeNotificationListener(listener);
      }

      @Override
      public void sendNotification(Notification notification) {
         delegate.sendNotification(notification);
      }
   }
}
