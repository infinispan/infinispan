package org.infinispan.server.resp.json;

public enum LenType {
    OBJECT, STRING, ARRAY, UNKNOWN;
    private static final LenType[] CACHED_VALUES = values();

    public static LenType fromCommand(String command) {
        if (command.contains("ARR")) {
            return ARRAY;
        }
        if (command.contains("OBJ")) {
            return OBJECT;
        }
        if (command.contains("STR")) {
            return STRING;
        }
        return UNKNOWN;
    }

    public static LenType valueOf(int ordinal) {
        return CACHED_VALUES[ordinal];
    }
}
