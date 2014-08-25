package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.util.ReflectionHelper;

import java.util.Iterator;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ReflectionMatcherEvalContext extends MatcherEvalContext<Class<?>, ReflectionHelper.PropertyAccessor, String> {

   private final Class<?> entityType;

   public ReflectionMatcherEvalContext(Object instance) {
      super(instance);
      entityType = instance.getClass();
   }

   @Override
   public Class<?> getEntityType() {
      return entityType;
   }

   @Override
   protected void processAttributes(AttributeNode<ReflectionHelper.PropertyAccessor, String> node, Object instance) {
      for (AttributeNode<ReflectionHelper.PropertyAccessor, String> childAttribute : node.getChildren()) {
         if (instance == null) {
            processAttribute(childAttribute, null);
         } else {
            ReflectionHelper.PropertyAccessor accessor = childAttribute.getMetadata();
            if (accessor.isMultiple()) {
               Iterator valuesIt = accessor.getValueIterator(instance);
               if (valuesIt == null) {
                  // try to evaluate eventual 'is null' predicates for this null collection
                  processAttribute(childAttribute, null);
               } else {
                  while (valuesIt.hasNext()) {
                     Object attributeValue = valuesIt.next();
                     processAttribute(childAttribute, attributeValue);
                  }
               }
            } else {
               Object attributeValue = accessor.getValue(instance);
               processAttribute(childAttribute, attributeValue);
            }
         }
      }
   }

   private void processAttribute(AttributeNode<ReflectionHelper.PropertyAccessor, String> attributeNode, Object attributeValue) {
      attributeNode.processValue(attributeValue, this);
      processAttributes(attributeNode, attributeValue);
   }
}
