package org.infinispan.commons.configuration;

import java.util.Collections;
import java.util.List;

/**
 * Exposes information about attributes and sub-elements of a configuration.
 *
 * @since 10.0
 */
public interface ConfigurationInfo extends BaseConfigurationInfo {

   /**
    * @return the list of sub elements.
    */
   default List<ConfigurationInfo> subElements() {
      return Collections.emptyList();
   }

}
