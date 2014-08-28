package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;
import org.infinispan.objectfilter.impl.predicateindex.be.PredicateNode;
import org.infinispan.objectfilter.impl.util.IntervalTree;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds all predicates that are subscribed for a certain attribute. This class is not thread safe and leaves this
 * responsibility to the caller.
 *
 * @param <AttributeDomain> the type of the values of the attribute
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class Predicates<AttributeDomain extends Comparable<AttributeDomain>, AttributeId extends Comparable<AttributeId>> {

   /**
    * Holds all subscriptions for a single predicate.
    */
   private static class Subscriptions {

      private final Predicate predicate;

      /**
       * The callbacks of the subscribed predicates.
       */
      private final List<Subscription> subscriptions = new ArrayList<Subscription>();

      private Subscriptions(Predicate predicate) {
         this.predicate = predicate;
      }

      void add(Subscription subscription) {
         subscriptions.add(subscription);
      }

      void remove(Subscription subscription) {
         subscriptions.remove(subscription);
      }

      boolean isEmpty() {
         return subscriptions.isEmpty();
      }

      //todo [anistor] this is an improvement but still does not eliminate precessing of attributes that have only suspended subscribers
      boolean isActive(MatcherEvalContext<?, ?, ?> ctx) {
         return !predicate.isRepeated() || ctx.getSuspendedSubscriptionsCounter(predicate) < subscriptions.size();
      }
   }

   public static final class Subscription<AttributeId extends Comparable<AttributeId>> {

      private final PredicateNode<AttributeId> predicateNode;

      private final FilterSubscriptionImpl filterSubscription;

      private Subscription(PredicateNode<AttributeId> predicateNode, FilterSubscriptionImpl filterSubscription) {
         this.predicateNode = predicateNode;
         this.filterSubscription = filterSubscription;
      }

      private void handleValue(MatcherEvalContext<?, ?, ?> ctx, boolean isMatching) {
         FilterEvalContext filterEvalContext = ctx.getFilterEvalContext(filterSubscription);
         if (!predicateNode.isDecided(filterEvalContext)) {
            if (predicateNode.isNegated()) {
               isMatching = !isMatching;
            }
            if (isMatching || !predicateNode.getPredicate().isRepeated()) {
               predicateNode.handleChildValue(null, isMatching, filterEvalContext);
            }
         }
      }

      public PredicateNode<AttributeId> getPredicateNode() {
         return predicateNode;
      }
   }

   private final boolean useIntervals;

   /**
    * The predicates that have a condition based on an order relation (ie. intervals). This allows them to be
    * represented by an interval tree.
    */
   private IntervalTree<AttributeDomain, Subscriptions> orderedPredicates;

   /**
    * The predicates that are based on an arbitrary condition that is not an order relation.
    */
   private List<Subscriptions> unorderedPredicates;

   Predicates(boolean useIntervals) {
      this.useIntervals = useIntervals;
   }

   public void notifyMatchingSubscribers(final MatcherEvalContext<?, ?, ?> ctx, Object attributeValue) {
      if (orderedPredicates != null && attributeValue instanceof Comparable) {
         orderedPredicates.stab((AttributeDomain) attributeValue, new IntervalTree.NodeCallback<AttributeDomain, Subscriptions>() {
            @Override
            public void handle(IntervalTree.Node<AttributeDomain, Subscriptions> node) {
               Subscriptions subscriptions = node.value;
               if (subscriptions.isActive(ctx)) {
                  for (Subscription s : subscriptions.subscriptions) {
                     s.handleValue(ctx, true);
                  }
               }
            }
         });
      }

      if (unorderedPredicates != null) {
         for (int k = unorderedPredicates.size() - 1; k >= 0; k--) {
            Subscriptions subscriptions = unorderedPredicates.get(k);
            if (subscriptions.isActive(ctx)) {
               boolean isMatching = subscriptions.predicate.match(attributeValue);
               List<Subscription> s = subscriptions.subscriptions;
               for (int i = s.size() - 1; i >= 0; i--) {
                  s.get(i).handleValue(ctx, isMatching);
               }
            }
         }
      }
   }

   public Predicates.Subscription<AttributeId> addPredicateSubscription(PredicateNode predicateNode, FilterSubscriptionImpl filterSubscription) {
      Subscriptions subscriptions;
      Predicate<AttributeDomain> predicate = predicateNode.getPredicate();
      if (useIntervals && predicate instanceof IntervalPredicate) {
         if (orderedPredicates == null) {
            // in this case AttributeDomain extends Comparable for sure
            orderedPredicates = new IntervalTree<AttributeDomain, Subscriptions>();
         }
         IntervalTree.Node<AttributeDomain, Subscriptions> n = orderedPredicates.add(((IntervalPredicate) predicate).getInterval());
         if (n.value == null) {
            subscriptions = new Subscriptions(predicate);
            n.value = subscriptions;
         } else {
            subscriptions = n.value;
         }
      } else {
         subscriptions = null;
         if (unorderedPredicates == null) {
            unorderedPredicates = new ArrayList<Subscriptions>();
         } else {
            for (int i = 0; i < unorderedPredicates.size(); i++) {
               Subscriptions s = unorderedPredicates.get(i);
               if (s.predicate.equals(predicate)) {
                  subscriptions = s;
                  break;
               }
            }
         }
         if (subscriptions == null) {
            subscriptions = new Subscriptions(predicate);
            unorderedPredicates.add(subscriptions);
         }
      }
      Subscription<AttributeId> subscription = new Subscription<AttributeId>(predicateNode, filterSubscription);
      subscriptions.add(subscription);
      return subscription;
   }

   public void removePredicateSubscription(Subscription subscription) {
      Predicate<AttributeDomain> predicate = (Predicate<AttributeDomain>) subscription.predicateNode.getPredicate();
      if (useIntervals && predicate instanceof IntervalPredicate) {
         if (orderedPredicates != null) {
            IntervalTree.Node<AttributeDomain, Subscriptions> n = orderedPredicates.findNode(((IntervalPredicate) predicate).getInterval());
            if (n != null) {
               n.value.remove(subscription);
               if (n.value.isEmpty()) {
                  orderedPredicates.remove(n);
               }
            } else {
               throwIllegalStateException();
            }
         } else {
            throwIllegalStateException();
         }
      } else {
         if (unorderedPredicates != null) {
            for (int i = 0; i < unorderedPredicates.size(); i++) {
               Predicates.Subscriptions subscriptions = unorderedPredicates.get(i);
               if (subscriptions.predicate.equals(predicate)) {
                  subscriptions.remove(subscription);
                  if (subscriptions.isEmpty()) {
                     unorderedPredicates.remove(i);
                  }
                  break;
               }
            }
         } else {
            throwIllegalStateException();
         }
      }
   }

   public boolean isEmpty() {
      return (unorderedPredicates == null || unorderedPredicates.isEmpty())
            && (orderedPredicates == null || orderedPredicates.isEmpty());
   }

   private static void throwIllegalStateException() throws IllegalStateException {
      // this is not expected to happen unless a programming error slipped through
      throw new IllegalStateException("Reached an invalid state");
   }
}
