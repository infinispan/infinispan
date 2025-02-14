package org.infinispan.server.resp.commands.json;

import org.infinispan.server.resp.json.JSONUtil;

import java.util.List;

/**
 * Helper class for reading JSON command arguments.
 */
public final class JSONCommandArgumentReader {
    public static byte[] DEFAULT_COMMAND_PATH = { '.' };

    /**
     * A record representing command arguments, typically used for processing commands
     * that involve keys, paths, and JSON path operations.
     *
     * @param key       the primary key as a byte array
     * @param path      the path associated with the command as a byte array
     * @param jsonPath  the JSON path for extracting specific data as a byte array
     * @param isLegacy  a flag indicating whether the command follows a legacy format
     */
    record CommandArgs(byte[] key, byte[] path, byte[] jsonPath, boolean isLegacy){
    }

    /**
     * Reads and processes the given list of byte array arguments.
     * Converts the raw byte array data into a structured {@code CommandArgs} object.
     *
     * @param arguments the list of byte arrays representing command arguments
     * @return a {@code CommandArgs} object containing the parsed arguments
     */
    public static CommandArgs readCommandArgs(List<byte[]> arguments) {
        byte[] key = arguments.get(0);
        byte[] path = arguments.size() > 1 ? arguments.get(1) : DEFAULT_COMMAND_PATH;
        final byte[] jsonPath = JSONUtil.toJsonPath(path);
        boolean isLegacy = path != jsonPath;
        return new CommandArgs(key, path, jsonPath, isLegacy);
    }
}
