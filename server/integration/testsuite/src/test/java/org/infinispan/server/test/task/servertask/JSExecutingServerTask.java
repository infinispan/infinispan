package org.infinispan.server.test.task.servertask;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.scripting.impl.ScriptingManagerImpl;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.TaskContext;
import org.infinispan.test.TestingUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

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

        // Running a JS script is really designed for direct remote execution,
        // and hence it's expected to return a byte[] result for sending back
        // to remote client.
        // To deploy a Java server task that then runs a Javascript file is
        // not the common case, so it makes sense for such edge cases to do
        // the hard work.
        byte[] result = (byte[]) scriptingManager.runScript("/stream_serverTask.js",
              new TaskContext().cache(usedCache).marshaller(taskContext.getMarshaller().get())).get();
        return taskContext.getMarshaller().get().objectFromByteBuffer(result);
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
