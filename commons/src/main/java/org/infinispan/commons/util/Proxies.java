package org.infinispan.commons.util;

import java.lang.reflect.Method;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * Proxies is a collection of useful dynamic profixes. Internal use only.
 *
 * @author vladimir
 * @since 4.0
 */
public class Proxies {
   public static Object newCatchThrowableProxy(Object obj) {
        return java.lang.reflect.Proxy.newProxyInstance(obj.getClass().getClassLoader(),
                        getInterfaces(obj.getClass()), new CatchThrowableProxy(obj));
    }

   private static Class<?>[] getInterfaces(Class<?> clazz) {
      Class<?>[] interfaces = clazz.getInterfaces();
      if (interfaces.length > 0) {
         Class<?> superClass = clazz.getSuperclass();
         if (superClass != null && superClass.getInterfaces().length > 0) {
            Class<?>[] superInterfaces = superClass.getInterfaces();
            Class<?>[] clazzes = new Class[interfaces.length + superInterfaces.length];
            System.arraycopy(interfaces, 0, clazzes, 0, interfaces.length);
            System.arraycopy(superInterfaces, 0, clazzes, interfaces.length, superInterfaces.length);
            return clazzes;
         } else {
            return interfaces;
         }
      }
      Class<?> superclass = clazz.getSuperclass();
      if (!superclass.equals(Object.class))
         return superclass.getInterfaces();
      return ReflectionUtil.EMPTY_CLASS_ARRAY;
   }

   /**
    * CatchThrowableProxy is a wrapper around interface that does not allow any exception to be
    * thrown when invoking methods on that interface. All exceptions are logged but not propagated
    * to the caller.
    *
    *
    */
   static class CatchThrowableProxy implements java.lang.reflect.InvocationHandler {

        private static final Log log = LogFactory.getLog(CatchThrowableProxy.class);

        private Object obj;

        public static Object newInstance(Object obj) {
            return java.lang.reflect.Proxy.newProxyInstance(obj.getClass().getClassLoader(),
                            obj.getClass().getInterfaces(), new CatchThrowableProxy(obj));
        }

        private CatchThrowableProxy(Object obj) {
            this.obj = obj;
        }

        @Override
        public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
            Object result = null;
            try {
                result = m.invoke(obj, args);
            } catch (Throwable t) {
                log.ignoringException(m.getName(), t.getMessage(), t.getCause());
            } finally {
            }
            return result;
        }
    }
}
