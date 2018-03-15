package org.infinispan.persistence;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Spliterators;

import org.infinispan.api.APINonTxTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.DataContainer.ComputeAction;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;

/**
 * Test that ensure that when persistence is used with an always empty data container that various operations
 * are properly supported
 * @author wburns
 * @since 9.2
 */
public class CacheAPIPersistenceTest extends APINonTxTest {
   @Override
   protected void configure(ConfigurationBuilder builder) {
      // We use a mocked container, so nothing is written
      DataContainer mockContainer = mock(DataContainer.class);
      when(mockContainer.iterator()).thenReturn(Collections.emptyIterator());
      when(mockContainer.spliterator()).thenReturn(Spliterators.emptySpliterator());
      when(mockContainer.entrySet()).thenReturn(Collections.emptySet());
      when(mockContainer.keySet()).thenReturn(Collections.emptySet());
      when(mockContainer.values()).thenReturn(Collections.emptyList());
      InternalEntryFactory factory = new InternalEntryFactoryImpl();
      when(mockContainer.compute(any(), any())).then(invocation -> {
         Object key = invocation.getArgument(0);
         ComputeAction action = invocation.getArgument(1);
         return action.compute(key, null, factory);
      });
      builder
            .dataContainer()
               .dataContainer(mockContainer)
            .persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class)
                  .storeName(getClass().getName())
            .purgeOnStartup(true)
            ;
   }

   @Override
   public void testEvict() {
      // Ignoring test as we have nothing to evict
   }
}
