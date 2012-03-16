/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.factories.components;

import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.util.ReflectionUtil;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Persistable and cacheable metadata for JMX attributes
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class JmxAttributeMetadata implements Serializable {
   private String name;
   private String description;
   private boolean writable;
   private boolean useSetter;
   private String type;
   private boolean is;

   private JmxAttributeMetadata(ManagedAttribute annotation) {
      description = annotation.description();
      writable = annotation.writable();
   }

   public JmxAttributeMetadata(Field field) {
      this(field.getAnnotation(ManagedAttribute.class));
      name = field.getName();
      type = field.getType().toString();
   }

   public JmxAttributeMetadata(Method method) {
      this(method.getAnnotation(ManagedAttribute.class));
      useSetter = true;
      String methodName = method.getName();
      name = ReflectionUtil.extractFieldName(methodName);
      is = methodName.startsWith("is");
      if (methodName.startsWith("set")) {
         type = method.getParameterTypes()[0].getName();
      } else if (methodName.startsWith("get") || is) {
         type = method.getReturnType().getName();
      }
   }

   public String getName() {
      return name;
   }

   public String getDescription() {
      return description;
   }

   public boolean isWritable() {
      return writable;
   }

   public boolean isUseSetter() {
      return useSetter;
   }

   public String getType() {
      return type;
   }

   public boolean isIs() {
      return is;
   }

   @Override
   public String toString() {
      return "JmxAttributeMetadata{" +
            "name='" + name + '\'' +
            ", description='" + description + '\'' +
            ", writable=" + writable +
            ", type='" + type + '\'' +
            ", is=" + is +
            '}';
   }
}
