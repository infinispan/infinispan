package org.infinispan.factories;

import org.infinispan.iteration.impl.DistributedEntryRetriever;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.iteration.impl.LocalEntryRetriever;
import org.infinispan.factories.annotations.DefaultFactoryFor;

import java.util.concurrent.TimeUnit;

/**
 * Factory that allows creation of an {@link org.infinispan.iteration.impl.EntryRetriever} based on the provided
 * configuration.
 *
 * @author wburns
 * @since 7.0
 */
@DefaultFactoryFor(classes = {EntryRetriever.class})
public class EntryRetrieverFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      if (configuration.clustering().cacheMode().isDistributed()) {
         return (T) new DistributedEntryRetriever(configuration.clustering().stateTransfer().chunkSize(),
                                                  configuration.clustering().stateTransfer().timeout(), TimeUnit.MILLISECONDS);
      } else {
         return (T) new LocalEntryRetriever(configuration.clustering().stateTransfer().chunkSize(),
                                            configuration.clustering().stateTransfer().timeout(), TimeUnit.MILLISECONDS);
      }
   }
}
