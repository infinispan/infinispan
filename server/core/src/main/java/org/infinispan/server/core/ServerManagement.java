package org.infinispan.server.core;

import java.util.Set;

import org.infinispan.commons.configuration.ConfigurationInfo;

/**
 * @since 10.0
 */
public interface ServerManagement {

   ConfigurationInfo getConfiguration();

   void stop();

   Set<String> cacheManagersNames();

}
