package org.infinispan.scripting.utils;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.test.TestingUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.Buffer;
import java.nio.CharBuffer;

/**
 * Utility class containing general methods for use.
 */
public class ScriptingUtils {

    public static ScriptingManager getScriptingManager(EmbeddedCacheManager manager) {
        return manager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
    }

    public static void loadData(Cache<String, String> cache, String fileName) throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                ScriptingUtils.class.getResourceAsStream(fileName)))) {
            int chunkSize = 10;
            int chunkId = 0;

            CharBuffer cbuf = CharBuffer.allocate(1024 * chunkSize);
            while (bufferedReader.read(cbuf) >= 0) {
                Buffer buffer = cbuf.flip();
                String textChunk = buffer.toString();
                cache.put(fileName + (chunkId++), textChunk);
                cbuf.clear();
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
