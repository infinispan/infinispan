package org.infinispan.examples;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;

public class HotRodClientAuth {

    public static void main(String[] args) {
        // Create a RemoteCacheManager with default configuration
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.addServer().host("127.0.0.1").port(ConfigurationProperties.DEFAULT_HOTROD_PORT)
                .security().authentication()
                .username("username").password("changeme");
        try (RemoteCacheManager cacheManager = new RemoteCacheManager(builder.build())) {
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