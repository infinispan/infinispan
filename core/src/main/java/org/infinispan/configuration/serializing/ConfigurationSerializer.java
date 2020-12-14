package org.infinispan.configuration.serializing;

import org.infinispan.commons.configuration.io.ConfigurationWriter;

/**
 * @author Tristan Tarrant
 * @since 9.0
 */
public interface ConfigurationSerializer<T> {
   void serialize(ConfigurationWriter writer, T configuration);
}
