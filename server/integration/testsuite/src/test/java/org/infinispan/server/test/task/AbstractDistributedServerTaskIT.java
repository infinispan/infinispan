package org.infinispan.server.test.task;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.server.test.task.servertask.DistributedCacheUsingTask;
import org.infinispan.server.test.task.servertask.DistributedJSExecutingServerTask;
import org.infinispan.server.test.task.servertask.DistributedMapReduceServerTask;
import org.infinispan.server.test.task.servertask.DistributedTestServerTask;
import org.infinispan.server.test.task.servertask.JSExecutingServerTask;
import org.infinispan.server.test.task.servertask.LocalMapReduceServerTask;
import org.infinispan.tasks.ServerTask;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Abstract class holding tests for Remote Task Execution in Distributed mode.
 *
 * @author amanukya
 */
public abstract class AbstractDistributedServerTaskIT {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();
    private static List<RemoteCacheManager> managers = null;

    protected abstract List<RemoteInfinispanServer> getServers();
    protected static List<String> expectedServerList;
    protected static final String CACHE_NAME = DistributedMapReduceServerTask.CACHE_NAME;
    protected static final String CACHE_NAME_TX = DistributedCacheUsingTask.CACHE_NAME;

    @Before
    public void setUp() {
        if (managers == null) {
            Configuration conf;
            managers = new ArrayList<>();
            for (RemoteInfinispanServer server : getServers()) {
                conf = new ConfigurationBuilder().addServer().host(server.getHotrodEndpoint().getInetAddress().getHostName())
                        .port(server.getHotrodEndpoint().getPort()).build();
                managers.add(new RemoteCacheManager(conf, true));
            }
        }

        for (RemoteCacheManager rcm : managers) {
            rcm.getCache().clear();
            rcm.getCache(CACHE_NAME).clear();
            rcm.getCache(CACHE_NAME_TX).clear();
        }
    }

    @AfterClass
    public static void release() {
        if (managers != null && !managers.isEmpty()) {
            for (RemoteCacheManager manager : managers) {
                manager.stop();
            }
        }
    }

    protected static JavaArchive createJavaArchive() {
        JavaArchive jar = ShrinkWrap.create(JavaArchive.class);
        jar.addClass(DistributedTestServerTask.class);
        jar.addClass(DistributedCacheUsingTask.class);
        jar.addClass(DistributedMapReduceServerTask.class);
        jar.addClass(DistributedJSExecutingServerTask.class);
        jar.addClass(LocalMapReduceServerTask.class);
        jar.addClass(JSExecutingServerTask.class);
        jar.addAsServiceProvider(ServerTask.class, DistributedTestServerTask.class, DistributedCacheUsingTask.class,
                DistributedMapReduceServerTask.class, DistributedJSExecutingServerTask.class);

        return jar;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldGatherNodeNamesInRemoteTasks() throws Exception {
        Object resultObject = managers.get(0).getCache().execute(DistributedTestServerTask.NAME, Collections.emptyMap());
        assertNotNull(resultObject);
        List<String> result = (List<String>) resultObject;
        assertEquals(2, result.size());
        System.out.println("The RESULT IS: " + result);
        assertTrue("result list does not contain expected items.", result.containsAll(expectedServerList));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldThrowExceptionInRemoteTasks() throws Exception {
        Map<String, Boolean> params = new HashMap<String, Boolean>();
        params.put("throwException", true);

        exceptionRule.expect(HotRodClientException.class);
        exceptionRule.expectMessage("Intentionally Thrown Exception");

        managers.get(0).getCache().execute(DistributedTestServerTask.NAME, params);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldPutNewValueInRemoteCache() throws Exception {
        String key = "key";
        String value = "value";
        String paramValue = "parameter";

        Map<String, String> params = new HashMap<>();
        params.put(DistributedCacheUsingTask.PARAM_KEY, paramValue);
        managers.get(1).getCache(CACHE_NAME_TX);
        managers.get(0).getCache(CACHE_NAME_TX).put(key, value);

        managers.get(0).getCache(CACHE_NAME_TX).execute(DistributedCacheUsingTask.NAME, params);
        assertEquals("modified:modified:value:parameter:parameter", managers.get(0).getCache(CACHE_NAME_TX).get(key));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldExecuteMapReduceOnReplCacheViaTask() throws Exception {
        RemoteCache remoteCache = managers.get(1).getCache(DistributedMapReduceServerTask.CACHE_NAME);
        remoteCache.put(1, "word1 word2 word3");
        remoteCache.put(2, "word1 word2");
        remoteCache.put(3, "word1");

        List<Map<String, Long>> result = (List<Map<String, Long>>)remoteCache.execute(DistributedMapReduceServerTask.NAME, Collections.emptyMap());
        assertEquals(2, result.size());
        verifyMapReduceResult(result.get(0));
        verifyMapReduceResult(result.get(1));

    }

    @Test
    @Ignore(value="Is disabled until ISPN-6173 is fixed.")
    public void shouldExecuteMapReduceViaJavaScriptInTask() throws Exception {
        RemoteCache remoteCache = managers.get(1).getCache(DistributedJSExecutingServerTask.CACHE_NAME);
        remoteCache.put(1, "word1 word2 word3");
        remoteCache.put(2, "word1 word2");
        remoteCache.put(3, "word1");

        List<Map<String, Long>> result = (List<Map<String, Long>>)remoteCache.execute(DistributedJSExecutingServerTask.NAME, Collections.emptyMap());
        assertEquals(2, result.size());
        verifyMapReduceResult(result.get(0));
        verifyMapReduceResult(result.get(1));
    }

    private void verifyMapReduceResult(Map<String, Long> result) {
        assertEquals(3, result.size());
        assertEquals(3, result.get("word1").intValue());
        assertEquals(2, result.get("word2").intValue());
        assertEquals(1, result.get("word3").intValue());
    }
}
