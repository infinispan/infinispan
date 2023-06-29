package org.infinispan.commons.configuration.io;

/**
 * @since 15.0
 **/
public class ConfigurationResourceResolvers {
   public static final ConfigurationResourceResolver DEFAULT = new URLConfigurationResourceResolver(null);

   private ConfigurationResourceResolvers() {
   }
}
