package org.infinispan.config.parsing;

import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.config.GlobalConfiguration;

import java.util.Map;

/**
 * Implementations of this interface are responsible for parsing XML configuration files.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface XmlConfigurationParser {
   /**
    * Parses the default template configuration.
    *
    * @return a configuration instance representing the "default" block in the configuration file
    * @throws ConfigurationException if there is a problem parsing the configuration XML
    */
   Configuration parseDefaultConfiguration() throws ConfigurationException;

   /**
    * Parses and retrieves configuration overrides for named caches.
    *
    * @return a Map of Configuration overrides keyed on cache name
    * @throws ConfigurationException if there is a problem parsing the configuration XML
    */
   Map<String, Configuration> parseNamedConfigurations() throws ConfigurationException;

   /**
    * GlobalConfiguration would also have a reference to the template default configuration, accessible via {@link
    * org.infinispan.config.GlobalConfiguration#getDefaultConfiguration()}
    * <p/>
    * This is typically used to configure a {@link org.infinispan.manager.DefaultCacheManager}
    *
    * @return a GlobalConfiguration as parsed from the configuration file.
    */
   GlobalConfiguration parseGlobalConfiguration();
}
