package org.infinispan.search.mapper.impl;

import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoTypeMetadataContributor;
import org.hibernate.search.mapper.pojo.model.additionalmetadata.building.spi.PojoAdditionalMetadataCollectorTypeNode;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.infinispan.search.mapper.model.impl.InfinispanSimpleStringSetPojoPathFilterFactory;

class InfinispanEntityTypeContributor implements PojoTypeMetadataContributor {

   private final PojoRawTypeIdentifier<?> typeIdentifier;
   private final String entityName;

   InfinispanEntityTypeContributor(PojoRawTypeIdentifier<?> typeIdentifier, String entityName) {
      this.typeIdentifier = typeIdentifier;
      this.entityName = entityName;
   }

   @Override
   public void contributeAdditionalMetadata(PojoAdditionalMetadataCollectorTypeNode collector) {
      if (!typeIdentifier.equals(collector.typeIdentifier())) {
         // Entity metadata is not inherited; only contribute it to the exact type.
         return;
      }
      collector.markAsEntity(entityName, new InfinispanSimpleStringSetPojoPathFilterFactory());
   }
}
