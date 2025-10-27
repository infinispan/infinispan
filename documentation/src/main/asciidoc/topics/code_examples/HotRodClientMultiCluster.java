package org.infinispan.examples;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

public class HotRodClientMultiCluster {

    public static void main(String[] args) {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.security().authentication().username("username").password("changeme");
        builder.addCluster("cluster1").addClusterNodes("172.20.0.11:11222; 172.20.0.12:11222");
        builder.addCluster("cluster2").addClusterNodes("172.21.0.11:11222; 172.21.0.12:11222");

        try (RemoteCacheManager cacheManager = new RemoteCacheManager(builder.build())) {
            RemoteCache<String, String> cache = cacheManager.getCache();

            cacheManager.switchToCluster("cluster1");
            cache.put("KEY", "VALUE ON CLUSTER 1");
            String value = cache.get("KEY");
            System.out.println("  KEY = " + value);

            cacheManager.switchToCluster("cluster2");
            value = cache.get("KEY");
            System.out.println("  KEY = " + value);

            cache.put("KEY", "VALUE_ON_CLUSTER_2");
            value = cache.get("KEY");
            System.out.println("  KEY = " + value);

        } catch (Exception e) {
            System.err.println("Error connecting to {brandname} server: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
