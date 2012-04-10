/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.util;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.reflect.Method;

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

