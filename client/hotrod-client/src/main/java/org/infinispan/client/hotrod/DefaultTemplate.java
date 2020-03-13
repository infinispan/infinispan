package org.infinispan.client.hotrod;

/**
 * This enum lists the cache configuration templates names that are available in the server by default.
 *
 * @since 10
 * @author Katia Aresti
 */
public enum DefaultTemplate {
   LOCAL("org.infinispan.LOCAL"),
   REPL_SYNC("org.infinispan.REPL_SYNC"),
   REPL_ASYNC("org.infinispan.REPL_ASYNC"),
   DIST_SYNC("org.infinispan.DIST_SYNC"),
   DIST_ASYNC("org.infinispan.DIST_ASYNC"),
   INVALIDATION_SYNC("org.infinispan.INVALIDATION_SYNC"),
   INVALIDATION_ASYNC("org.infinispan.INVALIDATION_ASYNC"),
   SCATTERED_SYNC("org.infinispan.SCATTERED_SYNC")
   ;

   private final String name;

   DefaultTemplate(String name) {
      this.name = name;
   }

   /**
    * Use this method to retrieve the value of a template configured in the infinispan-defaults.xml file
    *
    * @return name of the template
    */
   public String getTemplateName() {
      return name;
   }
}
