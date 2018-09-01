package org.infinispan.query;

/**
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
public class FetchOptions {

   private FetchMode fetchMode = FetchMode.LAZY;

   private int fetchSize = 1;

   /**
    * Set the fetch mode to be used to fetch matching results
    * @param fetchMode
    * @return {@code this} to allow method chaining
    */
   public FetchOptions fetchMode(FetchMode fetchMode) {
      if (fetchMode == null) {
         throw new IllegalArgumentException("fetchMode should not be null");
      }
      this.fetchMode = fetchMode;
      return this;
   }

   /**
    * Set the fetch size for batch loading of matches
    * @param fetchSize
    * @return {@code this} to allow method chaining
    */
   public FetchOptions fetchSize(int fetchSize) {
      if (fetchSize < 1) {
         throw new IllegalArgumentException("fetchSize should be greater than 0");
      }
      this.fetchSize = fetchSize;
      return this;
   }

   /**
    * @return the selected fetch mode
    */
   public FetchMode getFetchMode() {
      return fetchMode;
   }

   /**
    * @return the used fetch size
    */
   public int getFetchSize() {
      return fetchSize;
   }

   /**
    * Specifies the fetching strategy for query results.
    */
   public enum FetchMode {

      /**
       * With eager mode all results are loaded as soon as the query is performed; this results in a larger initial
       * transfer of entries but no remote operations during iteration of the resultset.
       */
      EAGER,

      /**
       * With lazy loading the entries are not loaded until each one is specifically requested. If iterating on very
       * larger result sets this is recommended to avoid loading too many entries in the VM performing the iteration.
       */
      LAZY
   }
}
