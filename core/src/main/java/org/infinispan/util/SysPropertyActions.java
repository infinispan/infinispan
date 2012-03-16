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
 * Priviledged actions for the package
 *
 * @author Scott.Stark@jboss.org
 * @since 4.2
 */
public class SysPropertyActions {

   interface SysProps {

      SysProps NON_PRIVILEDGED = new SysProps() {
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

      SysProps PRIVILEDGED = new SysProps() {
         @Override
         public String getProperty(final String name, final String defaultValue) {
            PrivilegedAction action = new PrivilegedAction() {
               public Object run() {
                  return System.getProperty(name, defaultValue);
               }
            };
            return (String) AccessController.doPrivileged(action);
         }

         @Override
         public String getProperty(final String name) {
            PrivilegedAction action = new PrivilegedAction() {
               public Object run() {
                  return System.getProperty(name);
               }
            };
            return (String) AccessController.doPrivileged(action);
         }

         @Override
         public String setProperty(final String name, final String value) {
            PrivilegedAction action = new PrivilegedAction() {
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
         return SysProps.NON_PRIVILEDGED.getProperty(name, defaultValue);

      return SysProps.PRIVILEDGED.getProperty(name, defaultValue);
   }

   public static String getProperty(String name) {
      if (System.getSecurityManager() == null)
         return SysProps.NON_PRIVILEDGED.getProperty(name);

      return SysProps.PRIVILEDGED.getProperty(name);
   }

   public static String setProperty(String name, String value) {
      if (System.getSecurityManager() == null)
         return SysProps.NON_PRIVILEDGED.setProperty(name, value);

      return SysProps.PRIVILEDGED.setProperty(name, value);
   }

}
