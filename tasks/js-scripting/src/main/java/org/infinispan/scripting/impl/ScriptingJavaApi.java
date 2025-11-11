package org.infinispan.scripting.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.roastedroot.quickjs4j.annotations.Builtins;
import io.roastedroot.quickjs4j.annotations.GuestFunction;
import io.roastedroot.quickjs4j.annotations.HostFunction;
import io.roastedroot.quickjs4j.annotations.HostRefParam;
import io.roastedroot.quickjs4j.annotations.Invokables;
import io.roastedroot.quickjs4j.annotations.ReturnsHostRef;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;

@Builtins("from_java")
public class ScriptingJavaApi {
    @Invokables("from_js")
    interface JsApi {
        @GuestFunction
        Object process(JsonNode userInput); // TODO: how to properly use/inject params? - doublecheck the chain
    }

    private final EmbeddedCacheManager cacheManager;

    ScriptingJavaApi(EmbeddedCacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @HostFunction("get_cache")
    @ReturnsHostRef
    public Cache getCache(String name) {
        return cacheManager.getCache(name);
    }

    @HostFunction("get")
    public Object get(@HostRefParam Cache cache, String key) {
        return cache.get(key);
    }

    @HostFunction("put")
    public void put(@HostRefParam Cache cache, String key, Object value) {
        cache.put(key, value);
    }
}
