package org.infinispan.server.test.api;

import java.util.Properties;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.marshall.Marshaller;

public interface HotRodClientDriver<T extends HotRodClientDriver<T>> extends CommonTestClientDriver<T> {
   T withClientConfiguration(ConfigurationBuilder clientConfiguration);

   T withClientConfiguration(ClientIntelligence clientIntelligence);

   T withClientConfiguration(Properties properties);

   T withMarshaller(Class<? extends Marshaller> marshallerClass);

   <K, V> RemoteCache<K, V> create();

   <K, V> RemoteCache<K, V> create(String name);

   <K, V> RemoteCache<K, V> create(int index);

   <K, V> RemoteCache<K, V> create(int index, String name);

   RemoteCacheManager createRemoteCacheManager();

   RemoteCacheManager createRemoteCacheManager(int index);
}
