package org.infinispan.configuration.parsing;

import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.stream.Location;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class ParseUtils {

    private ParseUtils() {
    }

    public static Element nextElement(XMLStreamReader reader) throws XMLStreamException {
        if (reader.nextTag() == END_ELEMENT) {
            return null;
        }
        return Element.forName(reader.getLocalName());
    }

    /**
     * Get an exception reporting an unexpected XML element.
     * @param reader the stream reader
     * @return the exception
     */
    public static XMLStreamException unexpectedElement(final XMLStreamReader reader) {
        return new XMLStreamException("Unexpected element '" + reader.getName() + "' encountered", reader.getLocation());
    }

    /**
     * Get an exception reporting an unexpected end tag for an XML element.
     * @param reader the stream reader
     * @return the exception
     */
    public static XMLStreamException unexpectedEndElement(final XMLStreamReader reader) {
        return new XMLStreamException("Unexpected end of element '" + reader.getName() + "' encountered", reader.getLocation());
    }

    /**
     * Get an exception reporting an unexpected XML attribute.
     * @param reader the stream reader
     * @param index the attribute index
     * @return the exception
     */
    public static XMLStreamException unexpectedAttribute(final XMLStreamReader reader, final int index) {
        return new XMLStreamException("Unexpected attribute '" + reader.getAttributeName(index) + "' encountered",
                reader.getLocation());
    }

    /**
     * Get an exception reporting an invalid XML attribute value.
     * @param reader the stream reader
     * @param index the attribute index
     * @return the exception
     */
    public static XMLStreamException invalidAttributeValue(final XMLStreamReader reader, final int index) {
        return new XMLStreamException("Invalid value '" + reader.getAttributeValue(index) + "' for attribute '"
                + reader.getAttributeName(index) + "'", reader.getLocation());
    }

    /**
     * Get an exception reporting a missing, required XML attribute.
     * @param reader the stream reader
     * @param required a set of enums whose toString method returns the
     *        attribute name
     * @return the exception
     */
    public static XMLStreamException missingRequired(final XMLStreamReader reader, final Set<?> required) {
        final StringBuilder b = new StringBuilder();
        Iterator<?> iterator = required.iterator();
        while (iterator.hasNext()) {
            final Object o = iterator.next();
            b.append(o.toString());
            if (iterator.hasNext()) {
                b.append(", ");
            }
        }
        return new XMLStreamException("Missing required attribute(s): " + b, reader.getLocation());
    }

    /**
     * Get an exception reporting a missing, required XML child element.
     * @param reader the stream reader
     * @param required a set of enums whose toString method returns the
     *        attribute name
     * @return the exception
     */
    public static XMLStreamException missingRequiredElement(final XMLStreamReader reader, final Set<?> required) {
        final StringBuilder b = new StringBuilder();
        Iterator<?> iterator = required.iterator();
        while (iterator.hasNext()) {
            final Object o = iterator.next();
            b.append(o.toString());
            if (iterator.hasNext()) {
                b.append(", ");
            }
        }
        return new XMLStreamException("Missing required element(s): " + b, reader.getLocation());
    }

    /**
     * Checks that the current element has no attributes, throwing an
     * {@link javax.xml.stream.XMLStreamException} if one is found.
     * @param reader the reader
     * @throws javax.xml.stream.XMLStreamException if an error occurs
     */
    public static void requireNoAttributes(final XMLStreamReader reader) throws XMLStreamException {
        if (reader.getAttributeCount() > 0) {
            throw unexpectedAttribute(reader, 0);
        }
    }

    /**
     * Consumes the remainder of the current element, throwing an
     * {@link javax.xml.stream.XMLStreamException} if it contains any child
     * elements.
     * @param reader the reader
     * @throws javax.xml.stream.XMLStreamException if an error occurs
     */
    public static void requireNoContent(final XMLStreamReader reader) throws XMLStreamException {
        if (reader.hasNext() && reader.nextTag() != END_ELEMENT) {
            throw unexpectedElement(reader);
        }
    }

    /**
     * Get an exception reporting that an attribute of a given name has already
     * been declared in this scope.
     * @param reader the stream reader
     * @param name the name that was redeclared
     * @return the exception
     */
    public static XMLStreamException duplicateAttribute(final XMLStreamReader reader, final String name) {
        return new XMLStreamException("An attribute named '" + name + "' has already been declared", reader.getLocation());
    }

    /**
     * Get an exception reporting that an element of a given type and name has
     * already been declared in this scope.
     * @param reader the stream reader
     * @param name the name that was redeclared
     * @return the exception
     */
    public static XMLStreamException duplicateNamedElement(final XMLStreamReader reader, final String name) {
        return new XMLStreamException("An element of this type named '" + name + "' has already been declared",
                reader.getLocation());
    }

    /**
     * Read an element which contains only a single boolean attribute.
     * @param reader the reader
     * @param attributeName the attribute name, usually "value"
     * @return the boolean value
     * @throws javax.xml.stream.XMLStreamException if an error occurs or if the
     *         element does not contain the specified attribute, contains other
     *         attributes, or contains child elements.
     */
    public static boolean readBooleanAttributeElement(final XMLStreamReader reader, final String attributeName)
            throws XMLStreamException {
        requireSingleAttribute(reader, attributeName);
        final boolean value = Boolean.parseBoolean(reader.getAttributeValue(0));
        requireNoContent(reader);
        return value;
    }

    /**
     * Read an element which contains only a single string attribute.
     * @param reader the reader
     * @param attributeName the attribute name, usually "value" or "name"
     * @return the string value
     * @throws javax.xml.stream.XMLStreamException if an error occurs or if the
     *         element does not contain the specified attribute, contains other
     *         attributes, or contains child elements.
     */
    public static String readStringAttributeElement(final XMLStreamReader reader, final String attributeName)
            throws XMLStreamException {
        requireSingleAttribute(reader, attributeName);
        final String value = reader.getAttributeValue(0);
        requireNoContent(reader);
        return value;
    }

    /**
     * Require that the current element have only a single attribute with the
     * given name.
     * @param reader the reader
     * @param attributeName the attribute name
     * @throws javax.xml.stream.XMLStreamException if an error occurs
     */
    public static String requireSingleAttribute(final XMLStreamReader reader, final String attributeName)
            throws XMLStreamException {
        final int count = reader.getAttributeCount();
        if (count == 0) {
            throw missingRequired(reader, Collections.singleton(attributeName));
        }
        requireNoNamespaceAttribute(reader, 0);
        if (!attributeName.equals(reader.getAttributeLocalName(0))) {
            throw unexpectedAttribute(reader, 0);
        }
        if (count > 1) {
            throw unexpectedAttribute(reader, 1);
        }
        return reader.getAttributeValue(0);
    }

    /**
     * Require all the named attributes, returning their values in order.
     * @param reader the reader
     * @param attributeNames the attribute names
     * @return the attribute values in order
     * @throws javax.xml.stream.XMLStreamException if an error occurs
     */
    public static String[] requireAttributes(final XMLStreamReader reader, final String... attributeNames)
            throws XMLStreamException {
        final int length = attributeNames.length;
        final String[] result = new String[length];
        for (int i = 0; i < length; i++) {
            final String name = attributeNames[i];
            final String value = reader.getAttributeValue(null, name);
            if (value == null) {
                throw missingRequired(reader, Collections.singleton(name));
            }
            result[i] = value;
        }
        return result;
    }

    public static boolean isNoNamespaceAttribute(final XMLStreamReader reader, final int index) {
        String namespace = reader.getAttributeNamespace(index);
        // FIXME when STXM-8 is done, remove the null check
        return namespace == null || XMLConstants.NULL_NS_URI.equals(namespace);
    }

    public static void requireNoNamespaceAttribute(final XMLStreamReader reader, final int index)
            throws XMLStreamException {
        if (!isNoNamespaceAttribute(reader, index)) {
            throw unexpectedAttribute(reader, index);
        }
    }

    public static String getWarningMessage(final String msg, final Location location) {
        return String.format("Parsing problem at [row,col]:[%d ,%d]%nMessage: %s%n", location.getLineNumber(), location.getColumnNumber(), msg);
    }

    public static Namespace[] getNamespaceAnnotations(Class<? extends ConfigurationParser> cls) {
       Namespaces namespacesAnnotation = cls.getAnnotation(Namespaces.class);

       if (namespacesAnnotation != null) {
          return namespacesAnnotation.value();
       }

       Namespace namespaceAnnotation = cls.getAnnotation(Namespace.class);
       if (namespaceAnnotation != null) {
          return new Namespace[] { namespaceAnnotation };
       }

       return null;
    }

}
