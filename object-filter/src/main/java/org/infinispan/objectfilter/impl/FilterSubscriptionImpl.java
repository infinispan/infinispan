package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.impl.predicateindex.AttributeNode;
import org.infinispan.objectfilter.impl.predicateindex.PredicateIndex;
import org.infinispan.objectfilter.impl.predicateindex.be.BETree;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterSubscriptionImpl<AttributeId extends Comparable<AttributeId>> implements FilterSubscription {

   private final String entityTypeName;

   private final BETree beTree;

   private final FilterCallback callback;

   private final String[] projection;

   private final List<List<AttributeId>> translatedProjection;

   public FilterSubscriptionImpl(String entityTypeName, BETree beTree, List<String> projection, FilterCallback callback, List<List<AttributeId>> translatedProjection) {
      this.entityTypeName = entityTypeName;
      this.beTree = beTree;
      this.callback = callback;

      if (projection != null && !projection.isEmpty()) {
         this.projection = projection.toArray(new String[projection.size()]);
      } else {
         this.projection = null;
      }

      this.translatedProjection = translatedProjection;
   }

   public BETree getBETree() {    // todo [anistor] do not expose this
      return beTree;
   }

   @Override
   public String getEntityTypeName() {
      return entityTypeName;
   }

   @Override
   public FilterCallback getCallback() {   // todo [anistor] do not expose this
      return callback;
   }

   @Override
   public String[] getProjection() {
      return projection;
   }

   public void registerProjection(PredicateIndex<AttributeId> predicateIndex) {
      if (translatedProjection != null) {
         for (int i = 0; i < translatedProjection.size(); i++) {
            List<AttributeId> projectionPath = translatedProjection.get(i);
            AttributeNode<AttributeId> node = predicateIndex.addAttributeNodeByPath(projectionPath);
            node.addProjection(this, i);
         }
      }
   }

   public void unregisterProjection(PredicateIndex<AttributeId> predicateIndex) {
      if (translatedProjection != null) {
         for (List<AttributeId> projectionPath : translatedProjection) {
            AttributeNode<AttributeId> node = predicateIndex.getAttributeNodeByPath(projectionPath);
            node.removeProjections(this);
         }
      }
   }
}
