package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.SortField;
import org.infinispan.objectfilter.impl.predicateindex.FilterEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.Predicate;
import org.infinispan.objectfilter.impl.predicateindex.PredicateIndex;
import org.infinispan.objectfilter.impl.predicateindex.be.BENode;
import org.infinispan.objectfilter.impl.predicateindex.be.BETree;
import org.infinispan.objectfilter.impl.predicateindex.be.BETreeMaker;
import org.infinispan.objectfilter.impl.predicateindex.be.PredicateNode;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.util.StringHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * A registry for filters on the same type of entity.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class FilterRegistry<AttributeId extends Comparable<AttributeId>> {

   private final PredicateIndex<AttributeId> predicateIndex;

   private final List<FilterSubscriptionImpl> filterSubscriptions = new ArrayList<FilterSubscriptionImpl>();

   private final BETreeMaker<AttributeId> treeMaker;

   private final MetadataAdapter<?, AttributeId> metadataAdapter;

   public FilterRegistry(MetadataAdapter<?, AttributeId> metadataAdapter) {
      this.metadataAdapter = metadataAdapter;
      treeMaker = new BETreeMaker<AttributeId>(metadataAdapter);
      predicateIndex = new PredicateIndex<AttributeId>(metadataAdapter);
   }

   public String getTypeName() {
      return metadataAdapter.getTypeName();
   }

   public boolean isEmpty() {
      return filterSubscriptions.isEmpty();
   }

   /**
    * Executes the registered filters and notifies each one of them whether it was satisfied or not by the given
    * instance.
    *
    * @param ctx
    */
   public void match(MatcherEvalContext<AttributeId> ctx) {
      // try to match
      ctx.process(predicateIndex.getRoot());

      // notify subscribers
      for (FilterSubscriptionImpl s : filterSubscriptions) {
         FilterEvalContext filterEvalContext = ctx.getFilterEvalContext(s);
         if (filterEvalContext.getMatchResult()) {
            s.getCallback().onFilterResult(ctx.getInstance(), filterEvalContext.getProjection(), filterEvalContext.getSortProjection());
         }
      }
   }

   public FilterSubscriptionImpl addFilter(BooleanExpr normalizedFilter, List<String> projection, List<SortField> sortFields, FilterCallback callback) {
      List<List<AttributeId>> translatedProjections = null;
      if (projection != null && !projection.isEmpty()) {
         translatedProjections = new ArrayList<List<AttributeId>>(projection.size());
         for (String projectionPath : projection) {
            translatedProjections.add(metadataAdapter.translatePropertyPath(StringHelper.splitPropertyPath(projectionPath)));
         }
      }

      List<List<AttributeId>> translatedSortFields = null;
      if (sortFields != null && !sortFields.isEmpty()) {
         translatedSortFields = new ArrayList<List<AttributeId>>(sortFields.size());
         for (SortField sortField : sortFields) {
            translatedSortFields.add(metadataAdapter.translatePropertyPath(StringHelper.splitPropertyPath(sortField.getPath())));
         }
      }

      BETree beTree = treeMaker.make(normalizedFilter);
      final FilterSubscriptionImpl<AttributeId> filterSubscription = new FilterSubscriptionImpl<AttributeId>(metadataAdapter, beTree, callback, projection, translatedProjections, sortFields, translatedSortFields);

      filterSubscription.registerProjection(predicateIndex);

      for (BENode node : beTree.getNodes()) {
         if (node instanceof PredicateNode) {
            final PredicateNode<AttributeId> predicateNode = (PredicateNode<AttributeId>) node;
            Predicate.Callback predicateCallback = new Predicate.Callback() {
               @Override
               public void handleValue(MatcherEvalContext<?> ctx, boolean isMatching) {
                  FilterEvalContext filterEvalContext = ctx.getFilterEvalContext(filterSubscription);
                  predicateNode.handleChildValue(null, isMatching, filterEvalContext);
               }
            };
            predicateNode.subscribe(predicateIndex, predicateCallback);
         }
      }

      filterSubscriptions.add(filterSubscription);
      return filterSubscription;
   }

   public void removeFilter(FilterSubscription filterSubscription) {
      FilterSubscriptionImpl<AttributeId> filterSubscriptionImpl = (FilterSubscriptionImpl<AttributeId>) filterSubscription;
      filterSubscriptions.remove(filterSubscriptionImpl);

      filterSubscriptionImpl.unregisterProjection(predicateIndex);

      for (BENode node : filterSubscriptionImpl.getBETree().getNodes()) {
         if (node instanceof PredicateNode) {
            PredicateNode predicateNode = (PredicateNode) node;
            predicateNode.unsubscribe();
         }
      }
   }
}
