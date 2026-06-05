package org.infinispan.jcache;

import javax.management.MBeanServer;
import javax.management.ObjectName;

final class SecurityActions {

   static void registerMBean(Object mbean, ObjectName objectName, MBeanServer mBeanServer) throws Exception {
      mBeanServer.registerMBean(mbean, objectName);
   }

   static void unregisterMBean(ObjectName objectName, MBeanServer mBeanServer) throws Exception {
      mBeanServer.unregisterMBean(objectName);
   }

   private SecurityActions() {
   }
}
