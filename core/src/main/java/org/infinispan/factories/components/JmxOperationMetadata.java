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
import org.infinispan.jmx.annotations.Parameter;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Persistable and cacheable metadata for JMX operations
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class JmxOperationMetadata implements Serializable {
   private final String methodName;
   private final String operationName;
   private final JmxOperationParameter[] methodParameters;
   private final String description;
   private final String returnType;

   public JmxOperationMetadata(Method m) {
      methodName = m.getName();
      returnType = m.getReturnType().getName();
      Class<?>[] params = m.getParameterTypes();
      Annotation[][] annots = m.getParameterAnnotations();
      methodParameters = new JmxOperationParameter[params.length];
      for (int i = 0; i < params.length; i++) {
         Parameter annot = null;
         for (int j = 0; j < annots[i].length; j++) {
            if (annots[i][j] instanceof Parameter) {
               annot = (Parameter) annots[i][j];
               break;
            }
         }
         if (annot == null) {
            methodParameters[i] = new JmxOperationParameter("p" + i, params[i].getName(), null);
         } else {
            methodParameters[i] = new JmxOperationParameter(annot.name(), params[i].getName(), annot.description());
         }
      }
      ManagedOperation mo = m.getAnnotation(ManagedOperation.class);
      operationName = mo.name();
      description = mo != null ? mo.description() : null;
   }

   public String getDescription() {
      return description;
   }

   public String getOperationName() {
      return operationName.isEmpty() ? methodName : operationName;
   }

   public String getMethodName() {
      return methodName;
   }

   public JmxOperationParameter[] getMethodParameters() {
      return methodParameters;
   }

   public String getReturnType() {
      return returnType;
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
