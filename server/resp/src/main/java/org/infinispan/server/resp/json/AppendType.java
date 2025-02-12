package org.infinispan.server.resp.json;

public enum AppendType {
    STRING, ARRAY, UNKNOWN;
    private static final AppendType[] CACHED_VALUES = values();

    public static AppendType fromCommand(String command) {
        if (command.contains("ARR")) {
            return ARRAY;
        }
        if (command.contains("STR")) {
            return STRING;
        }
        return UNKNOWN;
    }

    public static AppendType valueOf(int ordinal) {
        return CACHED_VALUES[ordinal];
    }
}
