package org.infinispan.query.dsl.embedded.impl;

import org.hibernate.search.engine.search.projection.SearchProjection;
import org.hibernate.search.engine.search.projection.dsl.SearchProjectionFactory;
import org.infinispan.search.mapper.common.EntityReference;

final class SearchProjectionInfo {

   static SearchProjectionInfo entity(SearchProjectionFactory<EntityReference, ?> factory) {
      return new SearchProjectionInfo(factory.entity().toProjection(), true);
   }

   static SearchProjectionInfo entityReference(SearchProjectionFactory<EntityReference, ?> factory) {
      return new SearchProjectionInfo(factory.entityReference().toProjection(), false);
   }

   static SearchProjectionInfo field(SearchProjectionFactory<EntityReference, ?> factory,
                                     String absoluteFieldPath, Class<?> type) {
      return new SearchProjectionInfo(factory.field(absoluteFieldPath, type).toProjection(), false);
   }

   static SearchProjectionInfo multiField(SearchProjectionFactory<EntityReference, ?> factory,
                                     String absoluteFieldPath, Class<?> type) {
      return new SearchProjectionInfo(factory.field(absoluteFieldPath, type).multi().toProjection(), false);
   }

   static SearchProjectionInfo composite(SearchProjectionFactory<EntityReference, ?> factory,
                                         SearchProjection<?>[] children) {
      return new SearchProjectionInfo(factory.composite(children).toProjection(), false);
   }

   private final SearchProjection<?> searchProjection;
   private final boolean isEntityProjection;

   private SearchProjectionInfo(SearchProjection<?> searchProjection, boolean isEntityProjection) {
      this.searchProjection = searchProjection;
      this.isEntityProjection = isEntityProjection;
   }

   public SearchProjection<?> getProjection() {
      return searchProjection;
   }

   public boolean isEntityProjection() {
      return isEntityProjection;
   }
}
