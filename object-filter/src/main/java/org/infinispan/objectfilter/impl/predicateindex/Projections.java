package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The projections of an attribute.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class Projections {

   private List<Subscription> subscriptions = new ArrayList<Subscription>();

   private class Subscription {

      private final FilterSubscriptionImpl filterSubscription;

      private final int position;

      private Subscription(FilterSubscriptionImpl filterSubscription, int position) {
         this.filterSubscription = filterSubscription;
         this.position = position;
      }
   }

   void addProjection(FilterSubscriptionImpl filterSubscription, int position) {
      subscriptions.add(new Subscription(filterSubscription, position));
   }

   void removeProjections(FilterSubscriptionImpl filterSubscription) {
      Iterator<Subscription> it = subscriptions.iterator();
      while (it.hasNext()) {
         Subscription s = it.next();
         if (s.filterSubscription == filterSubscription) {
            it.remove();
         }
      }
   }

   void processProjections(MatcherEvalContext<?> ctx, Object attributeValue) {
      for (Subscription s : subscriptions) {
         FilterEvalContext c = ctx.getFilterEvalContext(s.filterSubscription);
         Object[] projection = c.getProjection();
         int position = s.position;
         if (projection == null) {
            projection = c.getSortProjection();
         } else {
            if (position >= projection.length) {
               position -= projection.length;
               projection = c.getSortProjection();
            }
         }

         // if this is a repeated attribute then use the first occurrence in order to be consistent with the Lucene based implementation
         if (projection[position] == null) {
            projection[position] = attributeValue;
         }
      }
   }

   public boolean hasProjections() {
      return !subscriptions.isEmpty();
   }
}
