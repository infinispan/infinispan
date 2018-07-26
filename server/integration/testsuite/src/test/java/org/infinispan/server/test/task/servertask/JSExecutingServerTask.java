package org.infinispan.server.test.task.servertask;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;

/**
 * Server Task executing JavaScript script.
 *
 * @author amanukya
 */
public class JSExecutingServerTask implements ServerTask {
    public static final String NAME = "jsexecutor_task";
    public static final String CACHE_NAME = "taskAccessible";
    public static final String CACHE_NAME_PARAMETER = "cacheName";

    private TaskContext taskContext;

    @Override
    public void setTaskContext(TaskContext taskContext) {
        this.taskContext = taskContext;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Object call() throws Exception {
        Cache cache = taskContext.getCache().get();
        EmbeddedCacheManager cacheManager = cache.getCacheManager();

        String cacheName = getCacheName();
        if (taskContext.getParameters().isPresent() && taskContext.getParameters().get().get(CACHE_NAME_PARAMETER) != null) {
            cacheName = (String) taskContext.getParameters().get().get(CACHE_NAME_PARAMETER);
        }
        Cache usedCache = cacheManager.getCache(cacheName);

        ScriptingManager scriptingManager = cacheManager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
        loadScript(scriptingManager, "/stream_serverTask.js");

        TaskContext taskContext = new TaskContext().cache(usedCache.getAdvancedCache()
              .withMediaType(APPLICATION_OBJECT_TYPE, APPLICATION_OBJECT_TYPE));
        return scriptingManager.runScript("/stream_serverTask.js", taskContext).get();
    }

    public String getCacheName() {
        return CACHE_NAME;
    }

    private void loadScript(ScriptingManager scriptingManager, String scriptName) throws IOException {
        try (InputStream is = JSExecutingServerTask.class.getResourceAsStream("/" + scriptName)) {
            StringBuilder sb = new StringBuilder();
            BufferedReader r = new BufferedReader(new InputStreamReader(is));
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                sb.append(line);
                sb.append("\n");
            }
            String script = sb.toString();

            scriptingManager.addScript(scriptName, script);
        }
    }
}
