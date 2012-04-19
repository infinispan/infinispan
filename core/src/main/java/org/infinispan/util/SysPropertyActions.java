/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Privileged actions for the package
 *
 * @author Scott.Stark@jboss.org
 * @since 4.2
 */
public class SysPropertyActions {

   interface SysProps {

      SysProps NON_PRIVILEGED = new SysProps() {
         @Override
         public String getProperty(final String name, final String defaultValue) {
            return System.getProperty(name, defaultValue);
         }

         @Override
         public String getProperty(final String name) {
            return System.getProperty(name);
         }

         @Override
         public String setProperty(String key, String value) {
            return System.setProperty(key, value);
         }
      };

      SysProps PRIVILEGED = new SysProps() {
         @Override
         public String getProperty(final String name, final String defaultValue) {
            PrivilegedAction<Object> action = new PrivilegedAction() {
               @Override
               public Object run() {
                  return System.getProperty(name, defaultValue);
               }
            };
            return (String) AccessController.doPrivileged(action);
         }

         @Override
         public String getProperty(final String name) {
            PrivilegedAction<Object> action = new PrivilegedAction() {
               @Override
               public Object run() {
                  return System.getProperty(name);
               }
            };
            return (String) AccessController.doPrivileged(action);
         }

         @Override
         public String setProperty(final String name, final String value) {
            PrivilegedAction<Object> action = new PrivilegedAction() {
               @Override
               public Object run() {
                  return System.setProperty(name, value);
               }
            };
            return (String) AccessController.doPrivileged(action);
         }
      };

      String getProperty(String name, String defaultValue);

      String getProperty(String name);

      String setProperty(String name, String value);
   }

   public static String getProperty(String name, String defaultValue) {
      if (System.getSecurityManager() == null)
         return SysProps.NON_PRIVILEGED.getProperty(name, defaultValue);

      return SysProps.PRIVILEGED.getProperty(name, defaultValue);
   }

   public static String getProperty(String name) {
      if (System.getSecurityManager() == null)
         return SysProps.NON_PRIVILEGED.getProperty(name);

      return SysProps.PRIVILEGED.getProperty(name);
   }

   public static String setProperty(String name, String value) {
      if (System.getSecurityManager() == null)
         return SysProps.NON_PRIVILEGED.setProperty(name, value);

      return SysProps.PRIVILEGED.setProperty(name, value);
   }

   interface SetThreadContextClassLoaderAction {

      ClassLoader setThreadContextClassLoader(Class cl);

      ClassLoader setThreadContextClassLoader(ClassLoader cl);

      SetThreadContextClassLoaderAction NON_PRIVILEGED = new SetThreadContextClassLoaderAction() {
          @Override
          public ClassLoader setThreadContextClassLoader(Class cl) {
              ClassLoader old = Thread.currentThread().getContextClassLoader();
              Thread.currentThread().setContextClassLoader(cl.getClassLoader());
              return old;
          }
          @Override
          public ClassLoader setThreadContextClassLoader(ClassLoader cl) {
             ClassLoader old = Thread.currentThread().getContextClassLoader();
             Thread.currentThread().setContextClassLoader(cl);
             return old;
          }
      };

      SetThreadContextClassLoaderAction PRIVILEGED = new SetThreadContextClassLoaderAction() {

          @Override
          public ClassLoader setThreadContextClassLoader(final Class cl) {
              return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
                  @Override
                public ClassLoader run() {
                      ClassLoader old = Thread.currentThread().getContextClassLoader();
                      Thread.currentThread().setContextClassLoader(cl.getClassLoader());
                      return old;
                  }
              });
          }

          @Override
          public ClassLoader setThreadContextClassLoader(final ClassLoader cl) {
              return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
                  @Override
                public ClassLoader run() {
                     ClassLoader old = Thread.currentThread().getContextClassLoader();
                     Thread.currentThread().setContextClassLoader(cl);
                     return old;
                  }
              });
          }
      };
  }

   public static ClassLoader setThreadContextClassLoader(Class cl) {
      if (System.getSecurityManager() == null) {
          return SetThreadContextClassLoaderAction.NON_PRIVILEGED.setThreadContextClassLoader(cl);
      } else {
          return SetThreadContextClassLoaderAction.PRIVILEGED.setThreadContextClassLoader(cl);
      }
  }

  public static ClassLoader setThreadContextClassLoader(ClassLoader cl) {
      if (System.getSecurityManager() == null) {
          return SetThreadContextClassLoaderAction.NON_PRIVILEGED.setThreadContextClassLoader(cl);
      } else {
          return SetThreadContextClassLoaderAction.PRIVILEGED.setThreadContextClassLoader(cl);
      }
  }

}
