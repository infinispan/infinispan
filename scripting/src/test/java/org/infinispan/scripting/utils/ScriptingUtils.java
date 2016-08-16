package org.infinispan.scripting.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import org.infinispan.commons.api.BasicCache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.test.TestingUtil;

/**
 * Utility class containing general methods for use.
 */
public class ScriptingUtils {

    public static ScriptingManager getScriptingManager(EmbeddedCacheManager manager) {
        return manager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
    }

    public static void loadData(BasicCache<String, String> cache, String fileName) throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                ScriptingUtils.class.getResourceAsStream(fileName)))) {
            String value = null;
            int chunkId = 0;
            while (!(value = bufferedReader.lines().limit(400).collect(Collectors.joining(" "))).isEmpty()) {
                cache.put(fileName + (chunkId++), value);
            }
        }
    }

    public static void loadScript(ScriptingManager scriptingManager, String fileName) throws IOException {
        if (!fileName.startsWith("/")) {
            fileName = "/" + fileName;
        }
        try (InputStream is = ScriptingUtils.class.getResourceAsStream(fileName)) {
            String script = TestingUtil.loadFileAsString(is);
            scriptingManager.addScript(fileName.replaceAll("\\/", ""), script);
        }
    }

}
