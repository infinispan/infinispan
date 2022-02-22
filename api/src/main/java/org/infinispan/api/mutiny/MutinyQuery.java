package org.infinispan.api.mutiny;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Parameterized Query builder
 *
 * @param <R> the result type for the query
 */
public interface MutinyQuery<R> {
   /**
    * Sets the named parameter to the specified value
    *
    * @param name
    * @param value
    * @return
    */
   MutinyQuery param(String name, Object value);

   /**
    * Skips the first specified number of results
    *
    * @param skip
    * @return
    */
   MutinyQuery skip(long skip);

   /**
    * Limits the number of results
    *
    * @param limit
    * @return
    */
   MutinyQuery limit(int limit);

   /**
    * Executes the query
    */
   Multi<R> find();

   /**
    * Deletes all entries which match the query.
    *
    * @return
    */
   Uni<Long> delete();
}
