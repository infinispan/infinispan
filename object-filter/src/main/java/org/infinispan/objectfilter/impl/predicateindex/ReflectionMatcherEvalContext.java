package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.util.ReflectionHelper;

import java.util.Iterator;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ReflectionMatcherEvalContext extends MatcherEvalContext<String> {

   public ReflectionMatcherEvalContext(Object instance) {
      super(instance);
      entityTypeName = instance.getClass().getCanonicalName();
   }

   @Override
   protected void processAttributes(AttributeNode<String> node, Object instance) {
      Iterator<AttributeNode<String>> children = node.getChildrenIterator();
      while (children.hasNext()) {
         AttributeNode<String> childAttribute = children.next();
         if (instance == null) {
            processAttribute(childAttribute, null);
         } else {
            ReflectionHelper.PropertyAccessor accessor = ReflectionHelper.getAccessor(instance.getClass(), childAttribute.getAttribute());
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

   private void processAttribute(AttributeNode<String> attributeNode, Object attributeValue) {
      attributeNode.processValue(attributeValue, this);
      processAttributes(attributeNode, attributeValue);
   }
}
