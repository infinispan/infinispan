package org.infinispan.query.spi;

import org.hibernate.search.query.engine.spi.TimeoutExceptionFactory;
import org.hibernate.search.spi.CustomTypeMetadata;
import org.hibernate.search.spi.IndexedTypeMap;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.SearchManager;
import org.infinispan.query.Transformer;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.impl.QueryDefinition;

public interface SearchManagerImplementor extends SearchManager {

   /**
    * Registers a {@link Transformer} for the supplied key class. When storing keys in cache that are neither simple
    * (String, int, ...) nor annotated with {@code @Transformable}, Infinispan-Query will need to know what {@link
    * Transformer} to use when transforming the keys to Strings. Clients must specify what Transformer to use for a
    * particular key class by registering it through this method.
    * <p>
    * WARNING: this method registers the transformer on the local node only (see https://issues.jboss.org/browse/ISPN-9513)
    * and its usage is not recommended. Please configure transformers using the configuration API instead or the {@link
    * org.infinispan.query.Transformable} annotation.
    *
    * @param keyClass         the key class for which the supplied transformerClass should be used
    * @param transformerClass the transformer class to use for the supplied key class
    * @deprecated since 10.0
    */
   @Deprecated
   void registerKeyTransformer(Class<?> keyClass, Class<? extends Transformer> transformerClass);

   /**
    * Define the timeout exception factory to customize the exception thrown when the query timeout is exceeded.
    *
    * @param timeoutExceptionFactory the timeout exception factory to use
    */
   void setTimeoutExceptionFactory(TimeoutExceptionFactory timeoutExceptionFactory);

   /**
    * Creates a cache query based on a {@link QueryDefinition} and a custom metadata.
    */
   <E> CacheQuery<E> getQuery(QueryDefinition queryDefinition, IndexedQueryMode indexedQueryMode, IndexedTypeMap<CustomTypeMetadata> indexedTypeMap);
}
