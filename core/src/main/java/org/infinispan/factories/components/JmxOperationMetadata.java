package org.infinispan.factories.components;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;

/**
 * Persistable and cacheable metadata for JMX operations
 *
 * @author Manik Surtani
 * @since 5.1
 */
public final class JmxOperationMetadata implements Serializable {
   private final String methodName;
   private final String operationName;
   private final JmxOperationParameter[] methodParameters;
   private final String description;
   private final String returnType;

   JmxOperationMetadata(Method m) {
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
      operationName = mo != null ? (mo.name().isEmpty() ? methodName : mo.name()) : methodName;
      description = mo != null ? mo.description() : null;
   }

   public String getDescription() {
      return description;
   }

   public String getOperationName() {
      return operationName;
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
