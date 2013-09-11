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

    STRING_KEYED_JDBC_STORE("stringKeyedJdbcStore"),
    BINARY_KEYED_JDBC_STORE("binaryKeyedJdbcStore"),
    MIXED_KEYED_JDBC_STORE("mixedKeyedJdbcStore"),

    CONNECTION_POOL("connectionPool"),
    DATA_SOURCE("dataSource"),
    SIMPLE_CONNECTION("simpleConnection"),

    BINARY_KEYED_TABLE("binaryKeyedTable"),
    STRING_KEYED_TABLE("stringKeyedTable"),

    DATA_COLUMN("dataColumn"),
    ID_COLUMN("idColumn"),
    TIMESTAMP_COLUMN("timestampColumn"),
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
