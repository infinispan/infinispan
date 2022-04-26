package org.infinispan.search.mapper.mapping.impl;

import org.hibernate.search.mapper.pojo.identity.spi.IdentifierMapping;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoContainedTypeExtendedMappingCollector;
import org.hibernate.search.mapper.pojo.model.path.spi.PojoPathFilter;
import org.hibernate.search.mapper.pojo.model.spi.PojoPropertyModel;

/*
 * There's nothing here at the moment, just a placeholder.
 */
class InfinispanContainedTypeContext {

   private InfinispanContainedTypeContext() {
   }

   static class Builder implements PojoContainedTypeExtendedMappingCollector {
      Builder() {
      }

      @Override
      public void documentIdSourceProperty(PojoPropertyModel<?> documentIdSourceProperty) {

      }

      @Override
      public void identifierMapping(IdentifierMapping identifierMapping) {

      }

      @Override
      public void dirtyFilter(PojoPathFilter dirtyFilter) {

      }
   }
}
