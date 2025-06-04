package org.infinispan.configuration.parsing;

import static org.infinispan.commons.util.StringPropertyReplacer.replaceProperties;
import static org.infinispan.util.logging.Log.CONFIG;

import java.io.File;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiPredicate;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationReaderException;
import org.infinispan.commons.util.Util;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
public final class ParseUtils {

    private ParseUtils() {
    }

    public static Element nextElement(ConfigurationReader reader) throws ConfigurationReaderException {
        if (reader.nextElement() == ConfigurationReader.ElementType.END_ELEMENT) {
            return null;
        }
        return Element.forName(reader.getLocalName());
    }

    /**
     * Get an exception reporting an unexpected XML element.
     *
     * @param reader the stream reader
     * @return the exception
     */
    public static ConfigurationReaderException unexpectedElement(final ConfigurationReader reader) {
        return new ConfigurationReaderException("Unexpected element '" + reader.getLocalName() + "' encountered", reader.getLocation());
    }

    public static <T extends Enum<T>> ConfigurationReaderException unexpectedElement(final ConfigurationReader reader, T element) {
        return unexpectedElement(reader, element.toString());
    }

    public static ConfigurationReaderException unexpectedElement(final ConfigurationReader reader, String element) {
        return new ConfigurationReaderException("Unexpected element '" + element + "' encountered", reader.getLocation());
    }

    /**
     * Get an exception reporting an unexpected end tag for an XML element.
     *
     * @param reader the stream reader
     * @return the exception
     */
    public static ConfigurationReaderException unexpectedEndElement(final ConfigurationReader reader) {
        return new ConfigurationReaderException("Unexpected end of element '" + reader.getLocalName() + "' encountered", reader.getLocation());
    }

    /**
     * Get an exception reporting an unexpected XML attribute.
     *
     * @param reader the stream reader
     * @param index  the attribute index
     * @return the exception
     */
    public static ConfigurationReaderException unexpectedAttribute(final ConfigurationReader reader, final int index) {
        return new ConfigurationReaderException("Unexpected attribute '" + reader.getAttributeName(index) + "' encountered",
                reader.getLocation());
    }

    /**
     * Get an exception reporting an unexpected XML attribute.
     *
     * @param reader the stream reader
     * @param name   the attribute name
     * @return the exception
     */
    public static ConfigurationReaderException unexpectedAttribute(final ConfigurationReader reader, final String name) {
        return new ConfigurationReaderException("Unexpected attribute '" + name + "' encountered",
                reader.getLocation());
    }

    /**
     * Get an exception reporting an invalid XML attribute value.
     *
     * @param reader the stream reader
     * @param index  the attribute index
     * @return the exception
     */
    public static ConfigurationReaderException invalidAttributeValue(final ConfigurationReader reader, final int index) {
        return new ConfigurationReaderException("Invalid value '" + reader.getAttributeValue(index) + "' for attribute '"
                + reader.getAttributeName(index) + "'", reader.getLocation());
    }

    /**
     * Get an exception reporting a missing, required XML attribute.
     *
     * @param reader   the stream reader
     * @param required a set of enums whose toString method returns the attribute name
     * @return the exception
     */
    public static ConfigurationReaderException missingRequired(final ConfigurationReader reader, final Set<?> required) {
        final StringBuilder b = new StringBuilder();
        Iterator<?> iterator = required.iterator();
        while (iterator.hasNext()) {
            final Object o = iterator.next();
            b.append(o.toString());
            if (iterator.hasNext()) {
                b.append(", ");
            }
        }
        return new ConfigurationReaderException("Missing required attribute(s): " + b, reader.getLocation());
    }

    /**
     * Get an exception reporting a missing, required XML child element.
     *
     * @param reader   the stream reader
     * @param required a set of enums whose toString method returns the attribute name
     * @return the exception
     */
    public static ConfigurationReaderException missingRequiredElement(final ConfigurationReader reader, final Set<?> required) {
        final StringBuilder b = new StringBuilder();
        Iterator<?> iterator = required.iterator();
        while (iterator.hasNext()) {
            final Object o = iterator.next();
            b.append(o.toString());
            if (iterator.hasNext()) {
                b.append(", ");
            }
        }
        return new ConfigurationReaderException("Missing required element(s): " + b, reader.getLocation());
    }

    /**
     * Checks that the current element has no attributes, throwing an {@link ConfigurationReaderException} if one is
     * found.
     *
     * @param reader the reader
     * @throws ConfigurationReaderException if an error occurs
     */
    public static void requireNoAttributes(final ConfigurationReader reader) throws ConfigurationReaderException {
        if (reader.getAttributeCount() > 0) {
            throw unexpectedAttribute(reader, 0);
        }
    }

    /**
     * Consumes the remainder of the current element, throwing an {@link ConfigurationReaderException} if it contains
     * any child elements.
     *
     * @param reader the reader
     * @throws ConfigurationReaderException if an error occurs
     */
    public static void requireNoContent(final ConfigurationReader reader) throws ConfigurationReaderException {
        if (reader.hasNext() && reader.nextElement() != ConfigurationReader.ElementType.END_ELEMENT) {
            throw unexpectedElement(reader);
        }
    }

    /**
     * Get an exception reporting that an attribute of a given name has already been declared in this scope.
     *
     * @param reader the stream reader
     * @param name   the name that was redeclared
     * @return the exception
     */
    public static ConfigurationReaderException duplicateAttribute(final ConfigurationReader reader, final String name) {
        return new ConfigurationReaderException("An attribute named '" + name + "' has already been declared", reader.getLocation());
    }

    /**
     * Get an exception reporting that an element of a given type and name has already been declared in this scope.
     *
     * @param reader the stream reader
     * @param name   the name that was redeclared
     * @return the exception
     */
    public static ConfigurationReaderException duplicateNamedElement(final ConfigurationReader reader, final String name) {
        return new ConfigurationReaderException("An element of this type named '" + name + "' has already been declared",
                reader.getLocation());
    }

    /**
     * Read an element which contains only a single boolean attribute.
     *
     * @param reader        the reader
     * @param attributeName the attribute name, usually "value"
     * @return the boolean value
     * @throws ConfigurationReaderException if an error occurs or if the element does not contain the specified
     *                                      attribute, contains other attributes, or contains child elements.
     */
    public static boolean readBooleanAttributeElement(final ConfigurationReader reader, final String attributeName)
            throws ConfigurationReaderException {
        requireSingleAttribute(reader, attributeName);
        final boolean value = Boolean.parseBoolean(reader.getAttributeValue(0));
        requireNoContent(reader);
        return value;
    }

    /**
     * Read an element which contains only a single string attribute.
     *
     * @param reader        the reader
     * @param attributeName the attribute name, usually "value" or "name"
     * @return the string value
     * @throws ConfigurationReaderException if an error occurs or if the element does not contain the specified
     *                                      attribute, contains other attributes, or contains child elements.
     */
    public static String readStringAttributeElement(final ConfigurationReader reader, final String attributeName)
            throws ConfigurationReaderException {
        final String value = requireSingleAttribute(reader, attributeName);
        requireNoContent(reader);
        return value;
    }

    /**
     * Require that the current element have only a single attribute with the given name.
     *
     * @param reader        the reader
     * @param attributeName the attribute name
     * @throws ConfigurationReaderException if an error occurs
     */
    public static String requireSingleAttribute(final ConfigurationReader reader, final String attributeName)
            throws ConfigurationReaderException {
        final int count = reader.getAttributeCount();
        if (count == 0) {
            throw missingRequired(reader, Collections.singleton(attributeName));
        }
        requireNoNamespaceAttribute(reader, 0);
        if (!attributeName.equals(reader.getAttributeName(0))) {
            throw unexpectedAttribute(reader, 0);
        }
        if (count > 1) {
            throw unexpectedAttribute(reader, 1);
        }
        return reader.getAttributeValue(0);
    }

    public static String requireSingleAttribute(final ConfigurationReader reader, final Enum<?> attribute)
            throws ConfigurationReaderException {
        return requireSingleAttribute(reader, attribute.toString());
    }

    /**
     * Require all the named attributes, returning their values in order.
     *
     * @param reader         the reader
     * @param attributeNames the attribute names
     * @return the attribute values in order
     * @throws ConfigurationReaderException if an error occurs
     */
    public static String[] requireAttributes(final ConfigurationReader reader, boolean replace, final String... attributeNames)
            throws ConfigurationReaderException {
        final int length = attributeNames.length;
        final String[] result = new String[length];
        for (int i = 0; i < length; i++) {
            final String name = attributeNames[i];
            final String value = reader.getAttributeValue(name);
            if (value == null) {
                throw missingRequired(reader, Collections.singleton(name));
            }
            result[i] = replace ? replaceProperties(value) : value;
        }
        return result;
    }

    public static String[] requireAttributes(final ConfigurationReader reader, final String... attributeNames)
            throws ConfigurationReaderException {
        return requireAttributes(reader, false, attributeNames);
    }

    public static String[] requireAttributes(final ConfigurationReader reader, final Enum<?>... attributes)
            throws ConfigurationReaderException {
        String[] attributeNames = new String[attributes.length];
        for (int i = 0; i < attributes.length; i++) {
            attributeNames[i] = attributes[i].toString();
        }
        return requireAttributes(reader, true, attributeNames);
    }

    public static boolean isNoNamespaceAttribute(final ConfigurationReader reader, final int index) {
        String namespace = reader.getAttributeNamespace(index);
        return namespace == null || namespace.isEmpty();
    }

    public static void requireNoNamespaceAttribute(final ConfigurationReader reader, final int index)
            throws ConfigurationReaderException {
        if (!isNoNamespaceAttribute(reader, index)) {
            throw unexpectedAttribute(reader, index);
        }
    }

    public static Namespace[] getNamespaceAnnotations(Class<?> cls) {
        Namespaces namespacesAnnotation = cls.getAnnotation(Namespaces.class);

        if (namespacesAnnotation != null) {
            return namespacesAnnotation.value();
        }

        Namespace namespaceAnnotation = cls.getAnnotation(Namespace.class);
        if (namespaceAnnotation != null) {
            return new Namespace[]{namespaceAnnotation};
        }

        return null;
    }

    public static String resolvePath(String path, String relativeTo) {
        if (path == null) {
            return null;
        } else if (new File(path).isAbsolute()) {
            return path;
        } else if (relativeTo != null) {
            return new File(new File(relativeTo), path).getAbsolutePath();
        } else {
            return path;
        }
    }

    public static String requireAttributeProperty(final ConfigurationReader reader, int i) throws ConfigurationReaderException {
        String property = reader.getAttributeValue(i);
        Object value = reader.getProperty(property);
        if (value == null) {
            throw CONFIG.missingRequiredProperty(property, reader.getAttributeName(i), reader.getLocation());
        } else {
            return value.toString();
        }
    }

    public static void ignoreAttribute(ConfigurationReader reader, String attributeName) {
        CONFIG.ignoreAttribute(reader.getLocalName(), attributeName, reader.getLocation());
    }

    public static void ignoreAttribute(ConfigurationReader reader, int attributeIndex) {
        ignoreAttribute(reader, reader.getAttributeName(attributeIndex));
    }

    public static void ignoreAttribute(ConfigurationReader reader, Enum<?> attribute) {
        CONFIG.ignoreAttribute(reader.getLocalName(), attribute, reader.getLocation());
    }

    public static void ignoreElement(ConfigurationReader reader, Enum<?> element) {
        CONFIG.ignoreXmlElement(element, reader.getLocation());
    }

    public static CacheConfigurationException elementRemoved(ConfigurationReader reader) {
        return CONFIG.elementRemoved(reader.getLocalName(), reader.getLocation());
    }

    public static CacheConfigurationException attributeRemoved(ConfigurationReader reader, int attributeIndex) {
        String attributeName = reader.getAttributeName(attributeIndex);
        return CONFIG.attributeRemoved(reader.getLocalName(), attributeName, reader.getLocation());
    }

    public static void parseAttributes(ConfigurationReader reader, Builder<?> builder) {
        parseAttributes(reader, builder, null);
    }

    public static void parseAttributes(ConfigurationReader reader, Builder<?> builder, BiPredicate<String, String> invalidAttributeHandler) {
        AttributeSet attributes = builder.attributes();
        attributes.touch();
        int major = reader.getSchema().getMajor();
        int minor = reader.getSchema().getMinor();
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            String name = reader.getAttributeName(i);
            String value = reader.getAttributeValue(i);
            org.infinispan.commons.configuration.attributes.Attribute<Object> attribute = attributes.attribute(name);
            if (attribute == null) {
                if (attributes.isRemoved(name, major, minor)) {
                    CONFIG.ignoreAttribute(reader.getLocalName(), name, reader.getLocation());
                } else {
                    throw ParseUtils.unexpectedAttribute(reader, i);
                }
            } else {
                try {
                    attribute.fromString(value);
                    if (attribute.getAttributeDefinition().isDeprecated(major, minor)) {
                        CONFIG.attributeDeprecated(attribute.name(), attributes.getName(), major, minor);
                    }
                } catch (IllegalArgumentException e) {
                    if (invalidAttributeHandler == null || !invalidAttributeHandler.test(name, value)) {
                        throw CONFIG.invalidAttributeValue(reader.getLocalName(), name, value, reader.getLocation(), e.getLocalizedMessage());
                    }
                }
            }
        }
    }

    public static Integer parseInt(ConfigurationReader reader, int i, String value) {
        try {
            return Integer.parseInt(value);
        } catch (IllegalArgumentException e) {
            throw CONFIG.invalidAttributeValue(reader.getLocalName(), reader.getAttributeName(i), value, reader.getLocation(), e.getLocalizedMessage());
        }
    }

    public static long parseLong(ConfigurationReader reader, int i, String value) {
        try {
            return Long.parseLong(value);
        } catch (IllegalArgumentException e) {
            throw CONFIG.invalidAttributeValue(reader.getLocalName(), reader.getAttributeName(i), value, reader.getLocation(), e.getLocalizedMessage());
        }
    }

    public static <T extends Enum<T>> T parseEnum(ConfigurationReader reader, int i, Class<T> enumClass, String value) {
        try {
            return Enum.valueOf(enumClass, value);
        } catch (IllegalArgumentException e) {
            throw CONFIG.invalidAttributeEnumValue(reader.getLocalName(), reader.getAttributeName(i), value, EnumSet.allOf(enumClass).toString(), reader.getLocation());
        }
    }

    public static boolean parseBoolean(ConfigurationReader reader, int i, String value) {
        try {
            return Util.parseBoolean(value);
        } catch (IllegalArgumentException e) {
            throw CONFIG.invalidAttributeValue(reader.getLocalName(), reader.getAttributeName(i), value, reader.getLocation(), e.getLocalizedMessage());
        }
    }

    public static void introducedFrom(ConfigurationReader reader, int major, int minor) {
        if (!reader.getSchema().since(major, minor)) {
            throw ParseUtils.unexpectedElement(reader);
        }
    }

    public static void elementRemovedSince(ConfigurationReader reader, int major, int minor) {
        if (reader.getSchema().since(major, minor)) {
            throw ParseUtils.elementRemoved(reader);
        }
    }

    public static void attributeRemovedSince(ConfigurationReader reader, int major, int minor, int index) {
        if (reader.getSchema().since(major, minor)) {
            throw ParseUtils.attributeRemoved(reader, index);
        }
    }
}
