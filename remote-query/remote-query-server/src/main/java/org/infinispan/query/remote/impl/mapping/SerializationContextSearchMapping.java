package org.infinispan.query.remote.impl.mapping;

import java.util.Set;

import org.hibernate.search.engine.search.loading.spi.EntityLoader;
import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.ProgrammaticMappingConfigurationContext;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.impl.indexing.ProtobufEntityConverter;
import org.infinispan.query.remote.impl.mapping.model.ProtobufBootstrapIntrospector;
import org.infinispan.query.remote.impl.mapping.reference.GlobalReferenceHolder;
import org.infinispan.query.remote.impl.mapping.typebridge.ProtobufMessageBinder;
import org.infinispan.search.mapper.common.EntityReference;
import org.infinispan.search.mapper.mapping.SearchMappingBuilder;
import org.infinispan.search.mapper.mapping.SearchMappingCommonBuilding;

public final class SerializationContextSearchMapping {

   private SerializationContextSearchMapping() {
   }

   public static SearchMappingBuilder createBuilder(SearchMappingCommonBuilding commonBuilding,
                                                    EntityLoader<EntityReference, ?> entityLoader,
                                                    Set<String> indexedEntityTypes,
                                                    SerializationContext serializationContext) {
      GlobalReferenceHolder globalReferenceHolder = new GlobalReferenceHolder(serializationContext.getGenericDescriptors());
      ProtobufBootstrapIntrospector introspector = new ProtobufBootstrapIntrospector();
      SearchMappingBuilder builder = commonBuilding.builder(introspector);
      builder.setEntityLoader(entityLoader);
      builder.setEntityConverter(new ProtobufEntityConverter(serializationContext, globalReferenceHolder.getRootMessages()));
      ProgrammaticMappingConfigurationContext programmaticMapping = builder.programmaticMapping();

      boolean existIndexedEntities = false;
      for (GlobalReferenceHolder.RootMessageInfo rootMessage : globalReferenceHolder.getRootMessages()) {
         String fullName = rootMessage.getFullName();
         if (!indexedEntityTypes.contains(fullName)) {
            continue;
         }

         existIndexedEntities = true;

         programmaticMapping.type(fullName)
               .binder(new ProtobufMessageBinder(globalReferenceHolder, fullName))
               .indexed().index(rootMessage.getIndexName());

         builder.addEntityType(byte[].class, fullName);
      }

      return existIndexedEntities ? builder : null;
   }
}
