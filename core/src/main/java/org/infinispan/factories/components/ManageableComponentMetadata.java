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

import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.InfinispanCollections;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A specialization of {@link ComponentMetadata}, this version also includes JMX related metadata, as expressed
 * by {@link MBean}, {@link ManagedAttribute} and {@link ManagedOperation} annotations.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class ManageableComponentMetadata extends ComponentMetadata {

   private String jmxObjectName;
   private String description;
   private Set<JmxAttributeMetadata> attributeMetadata;
   private Set<JmxOperationMetadata> operationMetadata;

   public ManageableComponentMetadata(Class<?> component, List<Method> injectMethods, List<Method> startMethods, List<Method> stopMethods, boolean global, boolean survivesRestarts, List<Field> managedAttributeFields, List<Method> managedAttributeMethods, List<Method> managedOperationMethods, MBean mbean) {
      super(component, injectMethods, startMethods, stopMethods, global, survivesRestarts);
      if ((managedAttributeFields != null && !managedAttributeFields.isEmpty()) || (managedAttributeMethods != null && !managedAttributeMethods.isEmpty())) {
         attributeMetadata =  new HashSet<JmxAttributeMetadata>((managedAttributeFields == null ? 0 : managedAttributeFields.size()) + (managedAttributeMethods == null ? 0 : managedAttributeMethods.size()));
         
         if (managedAttributeFields != null) {
            for (Field f: managedAttributeFields) attributeMetadata.add(new JmxAttributeMetadata(f));
         }

         if (managedAttributeMethods != null) {
            for (Method m: managedAttributeMethods) attributeMetadata.add(new JmxAttributeMetadata(m));
         }
      }
      
      if (managedOperationMethods != null && !managedOperationMethods.isEmpty()) {
         operationMetadata = new HashSet<JmxOperationMetadata>(managedOperationMethods.size());
         for (Method m: managedOperationMethods) operationMetadata.add(new JmxOperationMetadata(m));
      }
      
      jmxObjectName = mbean.objectName();
      description = mbean.description();
   }

   public String getJmxObjectName() {
      return jmxObjectName;
   }

   public String getDescription() {
      return description;
   }

   public Set<JmxAttributeMetadata> getAttributeMetadata() {
      if (attributeMetadata == null) return InfinispanCollections.emptySet();
      return attributeMetadata;
   }

   public Set<JmxOperationMetadata> getOperationMetadata() {
      if (operationMetadata == null) return InfinispanCollections.emptySet();
      return operationMetadata;
   }

   @Override
   public boolean isManageable() {
      return true;
   }

   @Override
   public ManageableComponentMetadata toManageableComponentMetadata() {
      return this;
   }

   @Override
   public String toString() {
      return "ManageableComponentMetadata{" +
            "jmxObjectName='" + jmxObjectName + '\'' +
            ", description='" + description + '\'' +
            ", attributeMetadata=" + attributeMetadata +
            ", operationMetadata=" + operationMetadata +
            '}';
   }
}
