package org.infinispan.factories.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.factories.components.JmxAttributeMetadata;
import org.infinispan.factories.components.JmxOperationMetadata;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;

/**
 * JMX related component metadata,
 * as expressed by {@link MBean}, {@link ManagedAttribute} and {@link ManagedOperation} annotations.
 *
 * @author Dan Berindei
 * @since 10.0
 */
public class MBeanMetadata {
   private final String jmxObjectName;
   private final String description;
   private final String superMBeanClassName;
   private final Collection<JmxAttributeMetadata> attributes;
   private final Collection<JmxOperationMetadata> operations;

   public static MBeanMetadata of(String objectName, String description, String superMBeanClassName,
                                  Object... attributesAndOperations) {
      List<JmxAttributeMetadata> attributes = new ArrayList<>();
      List<JmxOperationMetadata> operations = new ArrayList<>();
      for (Object attributeOrOperation : attributesAndOperations) {
         if (attributeOrOperation instanceof JmxAttributeMetadata) {
            attributes.add((JmxAttributeMetadata) attributeOrOperation);
         } else if (attributeOrOperation instanceof JmxOperationMetadata) {
            operations.add((JmxOperationMetadata) attributeOrOperation);
         } else {
            throw new IllegalArgumentException();
         }
      }
      return new MBeanMetadata(objectName, description, superMBeanClassName, attributes, operations);
   }

   public MBeanMetadata(String jmxObjectName, String description, String superMBeanClassName,
                        Collection<JmxAttributeMetadata> attributes, Collection<JmxOperationMetadata> operations) {
      this.jmxObjectName = jmxObjectName != null ? (jmxObjectName.trim().length() == 0 ? null : jmxObjectName) : jmxObjectName;
      this.description = description;
      this.superMBeanClassName = superMBeanClassName;
      this.attributes = attributes;
      this.operations = operations;
   }

   public String getJmxObjectName() {
      return jmxObjectName;
   }

   public String getDescription() {
      return description;
   }

   public String getSuperMBeanClassName() {
      return superMBeanClassName;
   }

   public Collection<JmxAttributeMetadata> getAttributes() {
      return attributes;
   }

   public Collection<JmxOperationMetadata> getOperations() {
      return operations;
   }

   @Override
   public String toString() {
      return "MBeanMetadata{" +
             "jmxObjectName='" + jmxObjectName + '\'' +
             ", description='" + description + '\'' +
             ", super=" + superMBeanClassName +
             ", attributes=" + attributes +
             ", operations=" + operations +
             '}';
   }
}
