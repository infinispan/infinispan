package org.infinispan.examples;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

public class HotRodClientTLS {
    public static void main(String[] args) {
        ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
        clientBuilder.addServer().host("127.0.0.1").port(11222).security().ssl()
                // Clients use the trust store to verify {brandname} Server identities.
                .trustStoreFileName("/path/to/client.truststore")
                .trustStorePassword("changeme".toCharArray())
                // Clients present these certificates to {brandname} Server.
                .keyStoreFileName("/path/to/client.keystore")
                .keyStorePassword("keystorepassword".toCharArray());
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
