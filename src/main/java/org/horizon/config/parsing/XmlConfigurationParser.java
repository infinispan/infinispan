package org.horizon.config.parsing;

import org.horizon.config.Configuration;
import org.horizon.config.ConfigurationException;
import org.horizon.config.GlobalConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Implementations of this interface are responsible for parsing XML configuration files.
 *
 * @author Manik Surtani
 * @since 1.0
 */
public interface XmlConfigurationParser {
   /**
    * Initializes the parser with a String that represents the name of the configuration file to parse.  Parsers would
    * attempt to find this file on the classpath first, and failing that, treat the String as an absolute path name on
    * the file system.
    *
    * @param fileName name of file that contains the XML configuration
    * @throws java.io.IOException if there is a problem reading the configuration file
    */
   void initialize(String fileName) throws IOException;

   /**
    * Initializes the parser with a stream that contains the contents of an XML configuration file to parse.
    *
    * @param inputStream stream to read from
    * @throws IOException if there is a problem reading from the stream
    */
   void initialize(InputStream inputStream) throws IOException;

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
    * org.horizon.config.GlobalConfiguration#getDefaultConfiguration()}
    * <p/>
    * This is typically used to configure a {@link org.horizon.manager.DefaultCacheManager}
    *
    * @return a GlobalConfiguration as parsed from the configuration file.
    */
   GlobalConfiguration parseGlobalConfiguration();
}
