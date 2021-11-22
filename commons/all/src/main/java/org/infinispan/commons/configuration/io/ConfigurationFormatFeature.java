package org.infinispan.commons.configuration.io;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public enum ConfigurationFormatFeature {
   /**
    * The underlying format supports elements which can have both attributes and an array of child elements.
    * True for XML, false for JSON and YAML.
    */
   MIXED_ELEMENTS,
   /**
    * The underlying format supports collections without the need for wrapping in a container element.
    * True for XML, false for JSON and YAML.
    */
   BARE_COLLECTIONS;
}
