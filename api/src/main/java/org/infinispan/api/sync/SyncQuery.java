package org.infinispan.api.sync;

/**
 * Parameterized Query builder
 *
 * @param <R> the result type for the query
 */
public interface SyncQuery<R> {
   /**
    * Sets the named parameter to the specified value
    *
    * @param name
    * @param value
    * @return
    */
   SyncQuery param(String name, Object value);

   /**
    * Skips the first specified number of results
    *
    * @param skip
    * @return
    */
   SyncQuery skip(long skip);

   /**
    * Limits the number of results
    *
    * @param limit
    * @return
    */
   SyncQuery limit(int limit);

   /**
    * Executes the query
    */
   SyncQueryResult<R> find();

   /**
    * Removes all entries which match the query
    */
   void remove();

   /**
    * Processes entries matching the query
    */
   SyncQueryResult<R> process();
}
