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

import java.lang.reflect.Method;
import java.util.Locale;

/**
 * Simple JavaBean manipulation helper methods
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class BeanUtils {
   /**
    * Retrieves a setter name based on a field name passed in
    *
    * @param fieldName field name to find setter for
    * @return name of setter method
    */
   public static String setterName(String fieldName) {
      StringBuilder sb = new StringBuilder("set");
      if (fieldName != null && fieldName.length() > 0) {
         sb.append(fieldName.substring(0, 1).toUpperCase(Locale.ENGLISH));
         if (fieldName.length() > 1) {
            sb.append(fieldName.substring(1));
         }
      }
      return sb.toString();
   }

   /**
    * Returns a getter for a given class
    *
    * @param componentClass class to find getter for
    * @return name of getter method
    */
   public static String getterName(Class componentClass) {
      if (componentClass == null) return null;
      StringBuilder sb = new StringBuilder("get");
      sb.append(componentClass.getSimpleName());
      return sb.toString();
   }

   /**
    * Returns a setter for a given class
    *
    * @param componentClass class to find setter for
    * @return name of getter method
    */
   public static String setterName(Class componentClass) {
      if (componentClass == null) return null;
      StringBuilder sb = new StringBuilder("set");
      sb.append(componentClass.getSimpleName());
      return sb.toString();
   }


   /**
    * Returns a Method object corresponding to a getter that retrieves an instance of componentClass from target.
    *
    * @param target         class that the getter should exist on
    * @param componentClass component to get
    * @return Method object, or null of one does not exist
    */
   public static Method getterMethod(Class target, Class componentClass) {
      try {
         return target.getMethod(getterName(componentClass));
      }
      catch (NoSuchMethodException e) {
         //if (log.isTraceEnabled()) log.trace("Unable to find method " + getterName(componentClass) + " in class " + target);
         return null;
      }
      catch (NullPointerException e) {
         return null;
      }
   }

   /**
    * Returns a Method object corresponding to a setter that sets an instance of componentClass from target.
    *
    * @param target         class that the setter should exist on
    * @param componentClass component to set
    * @return Method object, or null of one does not exist
    */
   public static Method setterMethod(Class target, Class componentClass) {
      try {
         return target.getMethod(setterName(componentClass), componentClass);
      }
      catch (NoSuchMethodException e) {
         //if (log.isTraceEnabled()) log.trace("Unable to find method " + setterName(componentClass) + " in class " + target);
         return null;
      }
      catch (NullPointerException e) {
         return null;
      }
   }
}
