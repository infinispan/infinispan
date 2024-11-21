package org.infinispan.client.hotrod.impl.iteration;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.Arrays;
import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.iteration.MediaTypeMultiServerRemoteIteratorTest")
public class MediaTypeMultiServerRemoteIteratorTest extends BaseMultiServerRemoteIteratorTest {

   private static final int NUM_SERVERS = 3;

   private MediaType key = null;
   private MediaType value = null;

   private MediaTypeMultiServerRemoteIteratorTest withKeyType(MediaType key) {
      this.key = key;
      return this;
   }

   private MediaTypeMultiServerRemoteIteratorTest withValueType(MediaType value) {
      this.value = value;
      return this;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      if (key != null) builder.encoding().key().mediaType(key);
      if (value != null) builder.encoding().value().mediaType(value);
      return builder;
   }

   @Override
   protected <T> Map.Entry<Object, T> convertEntry(Map.Entry<Object, ?> entry) {
      return super.convertEntry(Map.entry(Integer.valueOf(entry.getKey().toString()), entry.getValue()));
   }

   @Override
   protected <T> T convertKey(Object key) {
      return super.convertKey(Integer.valueOf(key.toString()));
   }

   @Override
   public Object[] factory() {
      MediaType[] types = new MediaType[] {
            null,
            MediaType.APPLICATION_PROTOSTREAM,
            MediaType.TEXT_PLAIN,
            MediaType.APPLICATION_OBJECT,
      };

      return Arrays.stream(types)
            .flatMap(k -> Arrays.stream(types)
                  .map(v -> new MediaTypeMultiServerRemoteIteratorTest().withKeyType(k).withValueType(v)))
            .toArray();
   }

   @Override
   protected String parameters() {
      return String.format("[key=%s, value=%s]", key, value);
   }
}
