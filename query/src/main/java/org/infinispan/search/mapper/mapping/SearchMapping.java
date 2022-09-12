package org.infinispan.search.mapper.mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.hibernate.search.engine.reporting.FailureHandler;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.session.SearchSession;
import org.infinispan.search.mapper.work.SearchIndexer;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.NonBlockingManager;

public interface SearchMapping extends AutoCloseable {

   /**
    * Create a {@link SearchScope} limited to the given type.
    *
    * @param type A type to include in the scope.
    * @param <E>  An entity type to include in the scope.
    * @return The created scope.
    * @see SearchScope
    */
   default <E> SearchScope<E> scope(Class<E> type) {
      return scope(Collections.singleton(type));
   }

   /**
    * Create a {@link SearchScope} limited to the given types.
    *
    * @param types A collection of types to include in the scope.
    * @param <E>   An entity to include in the scope.
    * @return The created scope.
    * @see SearchScope
    */
   <E> SearchScope<E> scope(Collection<? extends Class<? extends E>> types);

   SearchScope<?> scopeAll();

   FailureHandler getFailureHandler();

   @Override
   void close();

   boolean isClose();

   default boolean isRestarting() {
      return false;
   }

   SearchSession getMappingSession();

   SearchIndexer getSearchIndexer();

   /**
    * @param entityType The type of an possible-indexed entity.
    * @return A {@link SearchIndexedEntity} for the indexed entity with the exact given type,
    *         if the type matches some indexed entity, otherwise {@code null}.
    */
   SearchIndexedEntity indexedEntity(Class<?> entityType);

   SearchIndexedEntity indexedEntity(String entityName);

   /**
    * @return A collection containing one {@link SearchIndexedEntity} for each indexed entity
    */
   Collection<? extends SearchIndexedEntity> allIndexedEntities();

   /**
    * @return A set containing the name of {@link #allIndexedEntities() all indexed entities}.
    */
   Set<String> allIndexedEntityNames();

   //TODO: ISPN-12449 replace with a method with a method returning entity names.
   // Currently this method returns java type, using byte[] to represents *all* protobuf types.
   // So if we use java types, we can't discriminate between two protobuf types,
   // which means for example that we can't reindex just one protobuf type;
   // see the callers for more details.
   Set<Class<?>> allIndexedEntityJavaClasses();

   /**
    * Releases all used resources (IO, threads)
    * and restarts from the mapping configuration.
    */
   default void reload() {
   }

   /**
    * Releases some used resources (e.g.: threads), preserving some others (e.g.: IO)
    * and restarts from the mapping configuration.
    */
   default void restart() {
   }

   /**
    * @param value An entity.
    * @return The internal Java class for this entity after conversion,
    * i.e. the Java class that will be returned by {@link #allIndexedEntityJavaClasses()}
    * if this entity is potentially indexed.
    * In practice, this is only useful to handle protobuf type: if an instance of ProtobufValueWrapper is passed,
    * this will return byte[] because that's the type we use for protobuf values internally.
    * For all other types, this just returns value.getClass().
    * @see EntityConverter
    */
   //TODO: ISPN-12449 this would be really simpler if we were just using entity names.
   // see allIndexedEntityJavaClasses.
   // However, there's a challenge here: we don't know the type of a ProtobufValueWrapper until it's deserialized,
   // and deserializing is costly so we don't want to deserialize it until we know we need to index it...
   Class<?> toConvertedEntityJavaClass(Object value);

   static SearchMappingBuilder builder(PojoBootstrapIntrospector introspector, ClassLoader aggregatedClassLoader,
                                       Collection<ProgrammaticSearchMappingProvider> mappingProviders,
                                       BlockingManager blockingManager, NonBlockingManager nonBlockingManager) {
      return new SearchMappingBuilder(introspector, aggregatedClassLoader, mappingProviders,
            blockingManager, nonBlockingManager);
   }
}
