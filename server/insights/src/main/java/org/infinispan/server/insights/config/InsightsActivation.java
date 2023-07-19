package org.infinispan.server.insights.config;

public enum InsightsActivation {

   /**
    * Full disabled
    */
   DISABLED,

   /**
    * Locally enabled - the report is accessible locally - not published remotely
    * The default value
    */
   LOCAL,

   /**
    *  Full enabled - publishing the report
    */
   ENABLED;

}
