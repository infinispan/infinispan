package org.infinispan.objectfilter.impl;

import org.infinispan.objectfilter.FilterCallback;
import org.infinispan.objectfilter.FilterSubscription;
import org.infinispan.objectfilter.impl.predicateindex.be.BETree;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterSubscriptionImpl implements FilterSubscription {

   protected final String entityTypeName;

   protected final BETree beTree;

   protected final FilterCallback callback;

   private final String[] projection;

   public FilterSubscriptionImpl(String entityTypeName, BETree beTree, List<String> projection, FilterCallback callback) {
      this.entityTypeName = entityTypeName;
      this.beTree = beTree;
      this.callback = callback;

      if (projection != null && !projection.isEmpty()) {
         this.projection = projection.toArray(new String[projection.size()]);
      } else {
         this.projection = null;
      }
   }

   public BETree getBETree() {    // todo [anistor] do not expose this
      return beTree;
   }

   public String getEntityTypeName() {
      return entityTypeName;
   }

   public FilterCallback getCallback() {   // todo [anistor] do not expose this
      return callback;
   }

   public String[] getProjection() {
      return projection;
   }
}
