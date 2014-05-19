package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
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

   private final String typeName;

   private final PredicateIndex<AttributeId> predicateIndex = new PredicateIndex<AttributeId>();

   private final List<FilterSubscriptionImpl> filterSubscriptions = new ArrayList<FilterSubscriptionImpl>();

   private final BETreeMaker<AttributeId> treeMaker;

   private final BETreeMaker.AttributePathTranslator<AttributeId> attributePathTranslator;

   public FilterRegistry(BETreeMaker.AttributePathTranslator<AttributeId> attributePathTranslator, String typeName) {
      this.attributePathTranslator = attributePathTranslator;
      this.typeName = typeName;
      treeMaker = new BETreeMaker<AttributeId>(attributePathTranslator);
   }

   public String getTypeName() {
      return typeName;
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
      ctx.process(predicateIndex.getRoot());

      for (FilterSubscriptionImpl s : filterSubscriptions) {
         FilterEvalContext filterEvalContext = ctx.getFilterEvalContext(s);
         if (filterEvalContext.getMatchResult()) {
            s.getCallback().onFilterResult(ctx.getInstance(), filterEvalContext.getProjection());
         }
      }
   }

   public FilterSubscriptionImpl addFilter(BooleanExpr normalizedFilter, List<String> projection, final FilterCallback callback) {
      List<List<AttributeId>> translatedProjections = null;
      if (projection != null && !projection.isEmpty()) {
         translatedProjections = new ArrayList<List<AttributeId>>(projection.size());
         for (String projectionPath : projection) {
            translatedProjections.add(attributePathTranslator.translatePath(StringHelper.splitPropertyPath(projectionPath)));
         }
      }

      BETree beTree = treeMaker.make(normalizedFilter);
      final FilterSubscriptionImpl<AttributeId> filterSubscription = new FilterSubscriptionImpl<AttributeId>(typeName, beTree, projection, callback, translatedProjections);

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
