package org.infinispan.config;

import java.util.Set;


/**
 * A registry for {@link Configuration}s.
 *
 * @author Brian Stansberry
 * @since 4.0
 */
public interface ConfigurationRegistry {
   /**
    * Gets a {@link Configuration#clone() clone} of the {@link Configuration} registered under the given name.
    * <p/>
    * The returned object is a clone of the internally held configuration, so any changes made to it by the caller will
    * not affect the internal state of this registry.
    *
    * @param configName the name of the configuration
    * @return a <code>Configuration</code>. Will not be <code>null</code>.
    * @throws IllegalArgumentException if no configuration is registered under <code>configName</code>
    */
   Configuration getConfiguration(String configName) throws Exception;

   /**
    * Register the given configuration under the given name.
    * <p/>
    * The configuration will be cloned before being stored internally, so the copy under the control of the registry
    * will not be affected by any external changes.
    *
    * @param configName the name of the configuration
    * @param config     the configuration
    * @throws CloneNotSupportedException
    * @throws IllegalStateException      if a configuration is already registered under <code>configName</code>
    */
   void registerConfiguration(String configName, Configuration config) throws CloneNotSupportedException;

   /**
    * Unregisters the named configuration.
    *
    * @param configName the name of the configuration
    * @throws IllegalStateException if no configuration is registered under <code>configName</code>
    */
   void unregisterConfiguration(String configName);

   /**
    * Gets the names of all registered configurations.
    *
    * @return a set of configuration names
    */
   Set<String> getConfigurationNames();
}