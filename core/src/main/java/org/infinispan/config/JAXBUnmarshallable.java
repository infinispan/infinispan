package org.infinispan.config;

/**
 * A mechanism to notify an XML element that the JAXB parser has willUnmarshall it
 *
 * @author Manik Surtani
 * @version 4.2
 */
public interface JAXBUnmarshallable {
   /**
    * Indicates that this element is about to be unmarshalled from the XML source that was processed.
    * @param parent parent component
    */
   void willUnmarshall(Object parent);
}
