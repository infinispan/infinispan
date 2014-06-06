package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.predicateindex.AttributeNode;
import org.infinispan.objectfilter.impl.predicateindex.PredicateIndex;
import org.infinispan.objectfilter.impl.predicateindex.be.BETree;
import org.infinispan.objectfilter.impl.util.ComparableArrayComparator;

import java.util.Comparator;
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

   private final SortField[] sortFields;

   private final List<List<AttributeId>> translatedSortProjection;

   private Comparator<Comparable[]> comparator;

   public FilterSubscriptionImpl(String entityTypeName, BETree beTree, FilterCallback callback,
                                 List<String> projection, List<List<AttributeId>> translatedProjection,
                                 List<SortField> sortFields, List<List<AttributeId>> translatedSortProjection) {
      this.entityTypeName = entityTypeName;
      this.beTree = beTree;
      this.callback = callback;

      if (projection != null && !projection.isEmpty()) {
         this.projection = projection.toArray(new String[projection.size()]);
      } else {
         this.projection = null;
      }

      if (sortFields != null && !sortFields.isEmpty()) {
         this.sortFields = sortFields.toArray(new SortField[sortFields.size()]);
      } else {
         this.sortFields = null;
      }

      this.translatedProjection = translatedProjection;
      this.translatedSortProjection = translatedSortProjection;
   }

   public BETree getBETree() {
      return beTree;
   }

   @Override
   public String getEntityTypeName() {
      return entityTypeName;
   }

   @Override
   public FilterCallback getCallback() {
      return callback;
   }

   @Override
   public String[] getProjection() {
      return projection;
   }

   @Override
   public SortField[] getSortFields() {
      return sortFields;
   }

   @Override
   public Comparator<Comparable[]> getComparator() {
      if (sortFields != null) {
         if (comparator == null) {
            boolean[] direction = new boolean[sortFields.length];
            for (int i = 0; i < sortFields.length; i++) {
               direction[i] = sortFields[i].isAscending();
            }
            comparator = new ComparableArrayComparator(direction);
         }
      }
      return comparator;
   }

   public void registerProjection(PredicateIndex<AttributeId> predicateIndex) {
      int i = 0;
      if (translatedProjection != null) {
         i = addProjections(predicateIndex, translatedProjection, i);
      }
      if (translatedSortProjection != null) {
         addProjections(predicateIndex, translatedSortProjection, i);
      }
   }

   public void unregisterProjection(PredicateIndex<AttributeId> predicateIndex) {
      if (translatedProjection != null) {
         removeProjections(predicateIndex, translatedProjection);
      }
      if (translatedSortProjection != null) {
         removeProjections(predicateIndex, translatedSortProjection);
      }
   }

   private int addProjections(PredicateIndex<AttributeId> predicateIndex, List<List<AttributeId>> projection, int i) {
      for (List<AttributeId> projectionPath : projection) {
         AttributeNode<AttributeId> node = predicateIndex.addAttributeNodeByPath(projectionPath);
         node.addProjection(this, i++);
      }
      return i;
   }

   private void removeProjections(PredicateIndex<AttributeId> predicateIndex, List<List<AttributeId>> projection) {
      for (List<AttributeId> projectionPath : projection) {
         AttributeNode<AttributeId> node = predicateIndex.getAttributeNodeByPath(projectionPath);
         node.removeProjections(this);
      }
   }
}
