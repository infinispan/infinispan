package org.infinispan.configuration.parsing;
/**
 * ParserScope indicates the configuration parser scope.
 * @author Tristan Tarrant
 * @since 9.0
 */
public enum ParserScope {
   /**
    * The top-level scope at which cache containers, jgroups and threads are defined
    */
   GLOBAL,
   /**
    * The cache-container scope
    */
   CACHE_CONTAINER,
   /**
    * The cache scope
    */
   CACHE,
   /**
    * The cache template scope
    */
   CACHE_TEMPLATE
}
