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

import java.util.ArrayList;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterRegistry<AttributeId extends Comparable<AttributeId>> {

   private final PredicateIndex<AttributeId> predicateIndex = new PredicateIndex<AttributeId>();

   private final List<FilterSubscriptionImpl> filterSubscriptions = new ArrayList<FilterSubscriptionImpl>();

   private final BETreeMaker<AttributeId> treeMaker;

   private final String typeName;

   public FilterRegistry(BETreeMaker.AttributePathTranslator<AttributeId> attributePathTranslator, String typeName) {
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
         // TODO [anistor] the instance here can be a byte[] or stream if the payload is protobuf encoded
         s.callback.onFilterResult(ctx.getInstance(), null, ctx.getFilterContext(s).getResult());   //todo projection
      }
   }

   public FilterSubscriptionImpl addFilter(BooleanExpr normalizedFilter, List<String> projection, final FilterCallback callback) {
      BETree beTree = treeMaker.make(normalizedFilter);

      final FilterSubscriptionImpl filterSubscription = new FilterSubscriptionImpl(typeName, beTree, projection, callback);

      for (BENode node : beTree.getNodes()) {
         if (node instanceof PredicateNode) {
            final PredicateNode<AttributeId> predicateNode = (PredicateNode<AttributeId>) node;
            Predicate.Callback predicateCallback = new Predicate.Callback() {
               @Override
               public void handleValue(MatcherEvalContext<?> ctx, boolean isMatching) {
                  FilterEvalContext context = ctx.getFilterContext(filterSubscription);
                  predicateNode.handleChildValue(predicateNode, isMatching, context);
               }
            };
            predicateNode.subscribe(predicateIndex, predicateCallback);
         }
      }

      filterSubscriptions.add(filterSubscription);
      return filterSubscription;
   }

   public void removeFilter(FilterSubscription filterSubscription) {
      FilterSubscriptionImpl filterSubscriptionImpl = (FilterSubscriptionImpl) filterSubscription;
      filterSubscriptions.remove(filterSubscriptionImpl);
      for (BENode node : filterSubscriptionImpl.getBETree().getNodes()) {
         if (node instanceof PredicateNode) {
            PredicateNode predicateNode = (PredicateNode) node;
            predicateNode.unsubscribe();
         }
      }
   }
}
