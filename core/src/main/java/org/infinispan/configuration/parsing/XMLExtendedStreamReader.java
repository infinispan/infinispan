package org.infinispan.configuration.parsing;

import java.util.Properties;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * An XML stream reader that can read nested {@code <xs:any>} content using a registered set of root
 * elements.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 * @since 6.0
 */
public interface XMLExtendedStreamReader extends XMLStreamReader {
   /**
    * Handle an {@code <xs:any>}-type nested element, passing in the given value, returning after
    * the end of the element. Must be positioned on a {@code START_ELEMENT} or an exception will
    * occur. On return the cursor will be positioned on the corresponding {@code END_ELEMENT}.
    *
    * @param holder a ConfigurationBuilderHolder
    * @throws XMLStreamException
    *            if an error occurs (e.g. the given value does not match the type of the handler for
    *            the element, or the element is unknown)
    */
   void handleAny(ConfigurationBuilderHolder holder) throws XMLStreamException;

   /**
    * Get the value of an attribute as a space-delimited string list.
    *
    * @param i the index of the attribute
    */
   String[] getListAttributeValue(int i);

   /**
    * Returns the schema of currently being processed
    *
    * @return schema the current schema
    */
   Schema getSchema();

   /**
    * Sets the current schema
    *
    * @param schema
    */
   void setSchema(Schema schema);

   /**
    * Returns the properties used for property-replacement
    *
    * @return the properties
    */
   Properties getProperties();

}
