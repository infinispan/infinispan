package org.infinispan.server.resp.json;

public enum NumOpType {
    INCR, MULT,UNKNOWN;
    private static final NumOpType[] CACHED_VALUES = values();

    public static NumOpType fromCommand(String command) {
        if (command.contains("INCR")) {
            return INCR;
        }
        if (command.contains("MULT")) {
            return MULT;
        }
        return UNKNOWN;
    }

    public static NumOpType valueOf(int ordinal) {
        return CACHED_VALUES[ordinal];
    }
}
