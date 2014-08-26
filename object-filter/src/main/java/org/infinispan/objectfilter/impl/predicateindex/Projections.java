package org.infinispan.objectfilter.impl.predicateindex;

import org.infinispan.objectfilter.impl.FilterSubscriptionImpl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The projections of an attribute across multiple filters.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class Projections {

   private List<ProjectionSubscription> subscriptions = new ArrayList<ProjectionSubscription>();

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
      for (int i = 0; i < subscriptions.size(); i++) {
         ProjectionSubscription s = subscriptions.get(i);
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

   boolean hasProjections() {
      return !subscriptions.isEmpty();
   }
}
