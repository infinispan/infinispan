package org.infinispan.factories.components;

import java.util.Arrays;

/**
 * Metadata for JMX operations.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public final class JmxOperationMetadata {

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
