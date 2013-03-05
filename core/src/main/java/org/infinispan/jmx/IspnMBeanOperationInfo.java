/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.jmx;

import javax.management.Descriptor;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import java.lang.reflect.Method;

/**
 * Infinispan allows a different JMX operation name than the actual method name that gets invoked
 * (see {@link org.infinispan.jmx.annotations.ManagedOperation#name()}.
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
