package org.infinispan.commons.configuration;

import java.util.Collection;
import java.util.Collections;

import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * Exposes attributes and element from a configuration builder.
 *
 * since 10.0
 */
public interface ConfigurationBuilderInfo extends BaseConfigurationInfo {

   /**
    * @return a collection of {@link ConfigurationBuilderInfo} for the sub-elements of the builder.
    */
   default Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return Collections.emptyList();
   }

   /**
    * @return the {@link ConfigurationBuilderInfo} associated with a certain serialized element name.
    */
   default ConfigurationBuilderInfo getBuilderInfo(String name, String qualifier) {
      if (getElementDefinition().supports(name)) return this;

      for (ConfigurationBuilderInfo childElement : this.getChildrenInfo()) {
         ElementDefinition<?> element = childElement.getElementDefinition();
         if (element != null && element.supports(name)) {
            return childElement;
         }

         boolean isTopLevel = element != null && element.isTopLevel();
         if (!isTopLevel) {
            ConfigurationBuilderInfo mergedChildren = childElement.getBuilderInfo(name, qualifier);
            if (mergedChildren != null) return mergedChildren;

         }
      }
      return null;
   }

   /**
    * @return same as {@link #getBuilderInfo(String, String)} but will return a new instance of the builder on every call.
    * This is to create unbounded sub-elements in a builder.
    */
   default ConfigurationBuilderInfo getNewBuilderInfo(String name) {
      return null;
   }
}
