package org.infinispan.query.objectfilter.impl.syntax;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
interface ToQueryString {

   /**
    * This method can be used only to log exceptions.
    * In any other case, use {@link #appendQueryString(StringBuilder)}.
    *
    * @return the query string
    */
   default String toQueryString() {
      StringBuilder sb = new StringBuilder();
      appendQueryString(sb);
      return sb.toString();
   }

   void appendQueryString(StringBuilder sb);

}
