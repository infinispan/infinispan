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
      Class<?> clazz = instance.getClass();
      Iterator<AttributeNode<String>> children = node.getChildrenIterator();
      while (children.hasNext()) {
         AttributeNode<String> childAttribute = children.next();
         if (isFieldMultiple(clazz, childAttribute.getAttribute())) {
            Iterator valuesIt = ReflectionHelper.getFieldIterator(instance, clazz, childAttribute.getAttribute());
            if (valuesIt != null) {
               while (valuesIt.hasNext()) {
                  Object attributeValue = valuesIt.next();
                  processAttribute(childAttribute, attributeValue);
               }
            }
         } else {
            Object attributeValue = ReflectionHelper.getPropertyValue(instance, clazz, childAttribute.getAttribute());
            processAttribute(childAttribute, attributeValue);
         }
      }
   }

   private void processAttribute(AttributeNode<String> attributeNode, Object attributeValue) {
      attributeNode.dispatchValueToPredicates(attributeValue, this);
      processAttributes(attributeNode, attributeValue);
   }

   private boolean isFieldMultiple(Class<?> clazz, String fieldName) {
      return ReflectionHelper.getElementType(clazz, fieldName) != null;
   }
}
