package org.infinispan.client.hotrod.configuration;

/**
 * Single port settings.
 */
public enum SinglePortMode {
   /**
    * Enabled.
    */
   ENABLED,

   /**
    * Disabled.
    */
   DISABLED,

   /**
    * Automatic mode. In this mode the Hotrod client tries to automatically discover single port on the
    * server by analyzing port. If the port is set to <code>80</code> or <code>8xxxx</code>, it will use
    * Single Port feature.
    */
   AUTO
}
