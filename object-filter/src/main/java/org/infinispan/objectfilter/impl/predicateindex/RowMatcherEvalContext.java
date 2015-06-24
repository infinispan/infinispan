package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.hql.RowPropertyHelper;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class RowMatcherEvalContext extends MatcherEvalContext<RowPropertyHelper.RowMetadata, RowPropertyHelper.ColumnMetadata, Integer> {

   private final RowPropertyHelper.RowMetadata rowMetadata;

   public RowMatcherEvalContext(Object userContext, Object instance, RowPropertyHelper.RowMetadata rowMetadata, Object eventType) {
      super(userContext, instance, eventType);
      this.rowMetadata = rowMetadata;
   }

   @Override
   public RowPropertyHelper.RowMetadata getEntityType() {
      return rowMetadata;
   }

   @Override
   protected void processAttributes(AttributeNode<RowPropertyHelper.ColumnMetadata, Integer> node, Object instance) {
      for (AttributeNode<RowPropertyHelper.ColumnMetadata, Integer> childAttribute : node.getChildren()) {
         Object attributeValue = null;
         if (instance != null) {
            attributeValue = childAttribute.getMetadata().getValue(instance);
         }
         childAttribute.processValue(attributeValue, this);
      }
   }
}
