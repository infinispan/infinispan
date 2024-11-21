package org.infinispan.client.hotrod;

import org.infinispan.commons.configuration.StringConfiguration;

/**
 * This enum lists the cache configuration templates names that are available in the server by default.
 *
 * @since 10
 * @author Katia Aresti
 */
@Deprecated(forRemoval = true)
public enum DefaultTemplate {
   LOCAL("org.infinispan.LOCAL", """
         {"local-cache": { "statistics": "true"}}
         """),
   REPL_SYNC("org.infinispan.REPL_SYNC", """
         {"replicated-cache": { "mode": "SYNC", "statistics": "true"}}
         """),
   REPL_ASYNC("org.infinispan.REPL_ASYNC", """
         {"replicated-cache": { "mode": "ASYNC", "statistics": "true"}}
         """),
   DIST_SYNC("org.infinispan.DIST_SYNC", """
         {"distributed-cache": { "mode": "SYNC", "statistics": "true"}}
         """),
   DIST_ASYNC("org.infinispan.DIST_ASYNC", """
         {"distributed-cache": { "mode": "ASYNC", "statistics": "true"}}
         """),
   INVALIDATION_SYNC("org.infinispan.INVALIDATION_SYNC", """
         {"invalidation-cache": { "mode": "SYNC", "statistics": "true"}}
         """),
   INVALIDATION_ASYNC("org.infinispan.INVALIDATION_ASYNC", """
         {"invalidation-cache": { "mode": "ASYNC", "statistics": "true"}}
         """),
   ;

   private final String name;
   private final StringConfiguration configuration;

   DefaultTemplate(String name, String configuration) {
      this.name = name;
      this.configuration = new StringConfiguration(configuration);
   }

   /**
    * Use this method to retrieve the value of a template configured in the infinispan-defaults.xml file
    *
    * @return name of the template
    */
   public String getTemplateName() {
      return name;
   }

   public StringConfiguration getConfiguration() {
      return configuration;
   }
}
