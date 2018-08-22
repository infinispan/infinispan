package org.infinispan.jmx;

import java.lang.reflect.Method;

import javax.management.Descriptor;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;

import org.infinispan.jmx.annotations.ManagedOperation;

/**
 * Infinispan allows a different JMX operation name than the actual method name that gets invoked
 * (see {@link ManagedOperation#name()}.
 * This class extends {@link MBeanOperationInfo} adding support for the operation name.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class IspnMBeanOperationInfo extends MBeanOperationInfo {

   final String operationName;

   public IspnMBeanOperationInfo(String description, Method method, String operationName) {
      super(description, method);
      this.operationName = operationName;
   }

   public IspnMBeanOperationInfo(String name, String description, MBeanParameterInfo[] signature, String type, int impact, String operationName) {
      super(name, description, signature, type, impact);
      this.operationName = operationName;
   }

   public IspnMBeanOperationInfo(String name, String description, MBeanParameterInfo[] signature, String type, int impact, Descriptor descriptor, String operationName) {
      super(name, description, signature, type, impact, descriptor);
      this.operationName = operationName;
   }

   public String getOperationName() {
      return operationName;
   }
}
