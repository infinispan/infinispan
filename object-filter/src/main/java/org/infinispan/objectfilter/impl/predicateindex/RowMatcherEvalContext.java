package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.syntax.parser.RowPropertyHelper;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class RowMatcherEvalContext extends MatcherEvalContext<RowPropertyHelper.RowMetadata, RowPropertyHelper.ColumnMetadata, Integer> {

   private final RowPropertyHelper.RowMetadata rowMetadata;

   public RowMatcherEvalContext(Object userContext, Object eventType, Object instance, RowPropertyHelper.RowMetadata rowMetadata) {
      super(userContext, eventType, instance);
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
