package org.infinispan.query.objectfilter.impl.predicateindex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.infinispan.query.objectfilter.impl.FilterSubscriptionImpl;

/**
 * The projections of an attribute across multiple filters.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class Projections {

   private final List<ProjectionSubscription> subscriptions = new ArrayList<>();

   private static class ProjectionSubscription {

      // the filter using this attribute subscription
      private final FilterSubscriptionImpl filterSubscription;

      // the position of the attribute in the projection
      private final int position;

      private ProjectionSubscription(FilterSubscriptionImpl filterSubscription, int position) {
         this.filterSubscription = filterSubscription;
         this.position = position;
      }
   }

   void addProjection(FilterSubscriptionImpl filterSubscription, int position) {
      subscriptions.add(new ProjectionSubscription(filterSubscription, position));
   }

   void removeProjections(FilterSubscriptionImpl filterSubscription) {
      Iterator<ProjectionSubscription> it = subscriptions.iterator();
      while (it.hasNext()) {
         ProjectionSubscription s = it.next();
         if (s.filterSubscription == filterSubscription) {
            it.remove();
         }
      }
   }

   void processProjections(MatcherEvalContext<?, ?, ?> ctx, Object attributeValue) {
      for (ProjectionSubscription s : subscriptions) {
         FilterEvalContext c = ctx.getFilterEvalContext(s.filterSubscription);
         c.processProjection(s.position, attributeValue);
      }
   }

   boolean hasProjections() {
      return !subscriptions.isEmpty();
   }
}
