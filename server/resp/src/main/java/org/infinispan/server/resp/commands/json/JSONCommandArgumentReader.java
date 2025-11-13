package org.infinispan.server.resp.commands.json;

import org.infinispan.server.resp.json.JSONUtil;

import java.util.List;

import static org.infinispan.server.resp.json.JSONUtil.JSON_ROOT;

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
     * @param isRoot    a flag indicating whether the command has root path
     */
    record CommandArgs(byte[] key, byte[] path, byte[] jsonPath, boolean isLegacy, boolean isRoot){
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
        return readCommandArgs(arguments, key, 1);
    }

    public static CommandArgs readCommandArgs(List<byte[]> arguments,  byte[] key , int pos) {
        byte[] path = arguments.size() > pos ? arguments.get(pos) : DEFAULT_COMMAND_PATH;
        final byte[] jsonPath = JSONUtil.toJsonPath(path);
        boolean isLegacy = !JSONUtil.isJsonPath(path);
        boolean isRoot = path.length == 1 && (path[0] == JSON_ROOT[0] || path[0] == DEFAULT_COMMAND_PATH[0]);
        return new CommandArgs(key, path, jsonPath, isLegacy, isRoot);
    }
}
