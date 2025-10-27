package org.infinispan.examples;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

public class HotRodClientSimplest {
    public static void main(String[] args) {
        try (RemoteCacheManager cacheManager = new RemoteCacheManager()) {
            RemoteCache<String, String> cache = cacheManager.getCache();

            cache.put("KEY", "VALUE");
            String value = cache.get("KEY");
            System.out.println("  KEY = " + value);

        } catch (Exception e) {
            System.err.println("Error connecting to {brandname} server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}