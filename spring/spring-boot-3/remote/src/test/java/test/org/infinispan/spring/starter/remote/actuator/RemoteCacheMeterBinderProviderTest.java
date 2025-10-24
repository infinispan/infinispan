package test.org.infinispan.spring.starter.remote.actuator;

import io.micrometer.core.instrument.binder.MeterBinder;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.api.BasicCache;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.starter.remote.actuator.RemoteInfinispanCacheMeterBinderProvider;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteCacheMeterBinderProviderTest {

   @Test
   public void getMeterBinderForRemoteCache() {

      RemoteInfinispanCacheMeterBinderProvider remoteInfinispanCacheMeterBinderProvider =
            new RemoteInfinispanCacheMeterBinderProvider();
      BasicCache<?, ?> nativeCache = mock(RemoteCache.class);
      when(nativeCache.getName()).thenReturn("cache");
      SpringCache cache = new SpringCache(nativeCache);

      MeterBinder meterBinder = remoteInfinispanCacheMeterBinderProvider.getMeterBinder(cache, null);

      assertThat(meterBinder).isNotNull();
   }

   @Test
   public void ingoreNonRemoteCache() {

      RemoteInfinispanCacheMeterBinderProvider remoteInfinispanCacheMeterBinderProvider =
            new RemoteInfinispanCacheMeterBinderProvider();
      BasicCache<?, ?> nativeCache = mock(BasicCache.class);
      when(nativeCache.getName()).thenReturn("cache");
      SpringCache cache = new SpringCache(nativeCache);

      MeterBinder meterBinder = remoteInfinispanCacheMeterBinderProvider.getMeterBinder(cache, null);

      assertThat(meterBinder).isNull();
   }
}
