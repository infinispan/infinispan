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
package org.infinispan.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * BeanConventions.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public class BeanConventions {

   public static String getPropertyFromBeanConvention(Method method) {
      String getterOrSetter = method.getName();
      if (getterOrSetter.startsWith("get") || getterOrSetter.startsWith("set")) {
         String withoutGet = getterOrSetter.substring(4);
         // not specifically BEAN convention, but this is what is bound in JMX.
         return Character.toUpperCase(getterOrSetter.charAt(3)) + withoutGet;
      } else if (getterOrSetter.startsWith("is")) {
         String withoutIs = getterOrSetter.substring(3);
         return Character.toUpperCase(getterOrSetter.charAt(2)) + withoutIs;
      }
      return getterOrSetter;
   }

   public static String getPropertyFromBeanConvention(Field field) {
      String fieldName = field.getName();
      String withoutFirstChar = fieldName.substring(1);
      return Character.toUpperCase(fieldName.charAt(0)) + withoutFirstChar;
   }

}
