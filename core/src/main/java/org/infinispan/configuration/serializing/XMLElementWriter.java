package org.infinispan.configuration.serializing;

import javax.xml.stream.XMLStreamException;

/**
 * @author David Lloyd
 * @since 9.0
 */
public interface XMLElementWriter<T> {

    /**
     * Write the contents of this item.
     *
     * @param streamWriter the stream writer
     * @param value the value passed in
     * @throws XMLStreamException if an exception occurs
     */
    void writeContent(XMLExtendedStreamWriter streamWriter, T value) throws XMLStreamException;

}