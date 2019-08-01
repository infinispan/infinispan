package org.infinispan.commons.configuration.elements;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;

/**
 * Defines a container for a set of configuration {@link Attribute}. It is usually represented as a top level element
 * when serialized as XML or a top level object for JSON and is associated with a Configuration instance.
 *
 * An ElementDefinition can contain zero or more sub-elements representing nested configurations.
 *
 */
public interface ElementDefinition<C extends ConfigurationInfo> {

   /**
    * @return true if the ElementDefinition is top-level or false if its attributes and children should be merged with the
    * parent element when serializing.
    */
   boolean isTopLevel();

   /**
    * @return the {@link ElementOutput} for serialization purposes of the configuration element.
    */
   ElementOutput toExternalName(C configuration);

   default boolean omitIfEmpty() {
      return true;
   }

   /**
    * @return true if a serialized elementName matches this ElementDefinition.
    */
   boolean supports(String elementName);

   /**
    * An ElementOutput specifies the name and optionally the class that an element must be serialized.
    * When className is present, it will be serialized as a 'class' attribute inside the element.
    */
   class ElementOutput {
      private final String name;
      private final String className;

      public ElementOutput(String name, String className) {
         this.name = name;
         this.className = className;
      }

      public ElementOutput(String name) {
         this.name = name;
         this.className = null;
      }
      public String getName() {
         return name;
      }
      public String getClassName() {
         return className;
      }
   }

}
