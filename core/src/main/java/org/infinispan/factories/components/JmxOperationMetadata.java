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
public class JmxOperationMetadata implements Serializable {
   private static final long serialVersionUID = 0x111402E12A71017L;
   private final String methodName;
   private final String operationName;
   private final String description;
   private final String returnType;
   private final JmxOperationParameter[] methodParameters;

   public JmxOperationMetadata(String methodName, String operationName, String description, String returnType,
                               JmxOperationParameter... methodParameters) {
      this.methodName = methodName;
      this.operationName = operationName.isEmpty() ? methodName : operationName;
      this.description = description;
      this.returnType = returnType;
      this.methodParameters = methodParameters;
   }

   /**
    * @deprecated Since 10.0, will be removed in 11 as the annotation is not available at runtime
    */
   @Deprecated
   public JmxOperationMetadata(Method m) {
      methodName = m.getName();
      returnType = m.getReturnType().getName();
      java.lang.reflect.Parameter[] params = m.getParameters();
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
         String paramType = params[i].getType().getName();
         if (annot == null) {
            String paramName = params[i].getName() != null ? params[i].getName() : "p" + i;
            methodParameters[i] = new JmxOperationParameter(paramName, paramType, null);
         } else {
            methodParameters[i] = new JmxOperationParameter(annot.name(), paramType, annot.description());
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
