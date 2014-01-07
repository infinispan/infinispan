package org.infinispan.persistence.jdbc.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * An enumeration of all the recognized XML element local names for the JDBC cache stores
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public enum Element {
    // must be first
    UNKNOWN(null),

    STRING_KEYED_JDBC_STORE("string-keyed-jdbc-store"),
    BINARY_KEYED_JDBC_STORE("binary-keyed-jdbc-store"),
    MIXED_KEYED_JDBC_STORE("mixed-keyed-jdbc-store"),

    CONNECTION_POOL("connection-pool"),
    DATA_SOURCE("data-source"),
    SIMPLE_CONNECTION("simple-connection"),

    BINARY_KEYED_TABLE("binary-keyed-table"),
    STRING_KEYED_TABLE("string-keyed-table"),

    DATA_COLUMN("data-column"),
    ID_COLUMN("id-column"),
    TIMESTAMP_COLUMN("timestamp-column"),
    ;

    private final String name;

    Element(final String name) {
        this.name = name;
    }

    /**
     * Get the local name of this element.
     *
     * @return the local name
     */
    public String getLocalName() {
        return name;
    }

    private static final Map<String, Element> MAP;

    static {
        final Map<String, Element> map = new HashMap<String, Element>(8);
        for (Element element : values()) {
            final String name = element.getLocalName();
            if (name != null) {
               map.put(name, element);
            }
        }
        MAP = map;
    }

    public static Element forName(final String localName) {
        final Element element = MAP.get(localName);
        return element == null ? UNKNOWN : element;
    }
}
