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

import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.ReflectionUtil;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Persistable and cacheable metadata for JMX operations
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class JmxOperationMetadata implements Serializable {
   private String methodName;
   private String[] methodParameters;
   private String description;
   
   public JmxOperationMetadata(Method m) {
      methodName = m.getName();
      Class[] params = m.getParameterTypes();
      methodParameters = ReflectionUtil.toStringArray(params);
      ManagedOperation mo = m.getAnnotation(ManagedOperation.class);
      if (mo != null) {
         description = mo.description();
      }
   }

   public String getDescription() {
      return description;
   }

   public String getMethodName() {
      return methodName;
   }

   public String[] getMethodParameters() {
      return methodParameters;
   }

   @Override
   public String toString() {
      return "JmxOperationMetadata{" +
            "methodName='" + methodName + '\'' +
            ", methodParameters=" + (methodParameters == null ? null : Arrays.asList(methodParameters)) +
            ", description='" + description + '\'' +
            '}';
   }
}
