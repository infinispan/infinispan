package org.infinispan.configuration.parsing;

import java.util.EventListener;

/**
 * ConfigurationParserListener. An interface which should be implemented by listeners who wish to be
 * notified when a file has been successfully parsed.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ConfigurationParserListener extends EventListener {
   void parsingComplete(ConfigurationBuilderHolder holder);
}
