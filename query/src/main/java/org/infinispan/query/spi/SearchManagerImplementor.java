package org.infinispan.query.spi;

import org.hibernate.search.query.engine.spi.TimeoutExceptionFactory;
import org.infinispan.query.SearchManager;
import org.infinispan.query.Transformer;

public interface SearchManagerImplementor extends SearchManager {

   /**
    * Registers a {@link org.infinispan.query.Transformer} for the supplied key class. When storing
    * keys in cache that are neither simple (String, int, ...) nor annotated with @Transformable,
    * Infinispan-Query will need to know what Transformer to use when transforming the keys to
    * Strings. Clients must specify what Transformer to use for a particular key class by
    * registering it through this method.
    * 
    * @param keyClass
    *           the key class for which the supplied transformerClass should be used
    * @param transformerClass
    *           the transformer class to use for the supplied key class
    */
   void registerKeyTransformer(Class<?> keyClass, Class<? extends Transformer> transformerClass);

   /**
    * Define the timeout exception factory to customize the exception thrown when the query timeout
    * is exceeded.
    * 
    * @param timeoutExceptionFactory
    *           the timeout exception factory to use
    */
   void setTimeoutExceptionFactory(TimeoutExceptionFactory timeoutExceptionFactory);

}
