package org.infinispan.query;

/**
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
public class FetchOptions {

   private FetchMode fetchMode;

   private int fetchSize;

   public FetchOptions(FetchMode fetchMode) {
      this(fetchMode, 1);
   }

   public FetchOptions(FetchMode fetchMode, int fetchSize) {
      this.fetchMode = fetchMode;
      this.fetchSize = fetchSize;
   }

   public FetchMode getFetchMode() {
      return fetchMode;
   }

   public int getFetchSize() {
      return fetchSize;
   }

   public static enum FetchMode {
      EAGER, LAZY
   }
}
