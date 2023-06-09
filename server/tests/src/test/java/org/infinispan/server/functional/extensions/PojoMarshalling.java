package org.infinispan.server.functional.extensions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class PojoMarshalling {

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @Test
   public void testPojoMarshalling() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addJavaSerialAllowList(".*");
      org.infinispan.configuration.cache.ConfigurationBuilder cacheBuilder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      // If you use JavaSerializationMarshaller or GenericJBossMarshaller you should encode caches with the application/x-java-serialized-object or application/x-jboss-marshalling media type, respectively.
      cacheBuilder.encoding().key().mediaType(MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE);
      cacheBuilder.encoding().value().mediaType(MediaType.APPLICATION_SERIALIZED_OBJECT_TYPE);
      cacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);
      RemoteCache<String, Person> cache = SERVERS.hotrod().withServerConfiguration(cacheBuilder).withClientConfiguration(builder).withMarshaller(JavaSerializationMarshaller.class).create();
      cache.put("123", new Person("Enrique", 29));
      Person person = cache.get("123");
      assertEquals("Enrique", person.getName());
      assertEquals(29, person.getAge());
   }
}
