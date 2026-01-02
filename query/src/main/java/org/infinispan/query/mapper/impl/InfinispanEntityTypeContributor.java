package org.infinispan.query.mapper.impl;

import java.util.Map;

import org.hibernate.search.engine.environment.bean.BeanReference;
import org.hibernate.search.engine.environment.bean.spi.ParameterizedBeanReference;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoTypeMetadataContributor;
import org.hibernate.search.mapper.pojo.model.additionalmetadata.building.spi.PojoAdditionalMetadataCollectorTypeNode;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;
import org.infinispan.query.mapper.search.loading.context.impl.InfinispanSelectionLoadingBinder;

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

      var node = collector.markAsEntity();
      node.entityName(entityName);
      InfinispanSelectionLoadingBinder loadingBinder = new InfinispanSelectionLoadingBinder();
      node.loadingBinder(ParameterizedBeanReference.of(BeanReference.ofInstance(loadingBinder), Map.of()));
   }
}
