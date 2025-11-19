package org.infinispan.scripting.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.roastedroot.quickjs4j.annotations.Builtins;
import io.roastedroot.quickjs4j.annotations.GuestFunction;
import io.roastedroot.quickjs4j.annotations.HostFunction;
import io.roastedroot.quickjs4j.annotations.HostRefParam;
import io.roastedroot.quickjs4j.annotations.Invokables;
import io.roastedroot.quickjs4j.annotations.ReturnsHostRef;
import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;

import java.util.Objects;

@Builtins("from_java")
public class ScriptingJavaApi {
    @Invokables("from_js")
    interface JsApi {
        @GuestFunction
        Object process(JsonNode userInput);
    }

    private final EmbeddedCacheManager cacheManager;
    private final Cache defaultCache;

    ScriptingJavaApi(EmbeddedCacheManager cacheManager, Cache defaultCache) {
        this.cacheManager = cacheManager;
        this.defaultCache = defaultCache;
    }

    @HostFunction("get_cache")
    @ReturnsHostRef
    public Cache getCache(String name) {
        return cacheManager.getCache(name);
    }

    @HostFunction("get_default_cache")
    @ReturnsHostRef
    public Cache getDefaultCache() {
        Objects.requireNonNull(defaultCache);
        return defaultCache;
    }

    // implementations of the functionality of: DataTypedCacheManager
    @HostFunction("entry_set")
    public CacheSet getEntrySet(@HostRefParam Cache cache) {
        return cache.entrySet();
    }

    @HostFunction("get")
    public Object get(@HostRefParam Cache cache, String key) {
        return cache.get(key);
    }

    @HostFunction("put")
    public void put(@HostRefParam Cache cache, String key, Object value) {
        cache.put(key, value);
    }

    @HostFunction("address")
    public Address address() {
        return cacheManager.getAddress();
    }
}
