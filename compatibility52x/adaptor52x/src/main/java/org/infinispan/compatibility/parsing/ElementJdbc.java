package org.infinispan.compatibility.parsing;

import java.util.HashMap;
import java.util.Map;

/**
 * An enumeration of all the recognized XML element local names for the JDBC cache stores
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public enum ElementJdbc {
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

    ElementJdbc(final String name) {
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

    private static final Map<String, ElementJdbc> MAP;

    static {
        final Map<String, ElementJdbc> map = new HashMap<String, ElementJdbc>(8);
        for (ElementJdbc elementJdbc : values()) {
            final String name = elementJdbc.getLocalName();
            if (name != null) {
               map.put(name, elementJdbc);
            }
        }
        MAP = map;
    }

    public static ElementJdbc forName(final String localName) {
        final ElementJdbc elementJdbc = MAP.get(localName);
        return elementJdbc == null ? UNKNOWN : elementJdbc;
    }
}
