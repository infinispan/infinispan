package org.infinispan.cdi.common.util;

import javax.enterprise.inject.spi.BeanManager;

public class CDIHelper {

   public static final boolean isCDIAvailable() {
      try {
         CDIHelper.class.getClassLoader().loadClass("javax.enterprise.inject.spi.BeanManager");
         return true;
      } catch(ClassNotFoundException e) {
         return false;
      }
   }

   public static final BeanManager getBeanManager() {
      try {
         return BeanManagerProvider.getInstance().getBeanManager();
      } catch (IllegalStateException ise) {
         return null;
      }
   }
}
