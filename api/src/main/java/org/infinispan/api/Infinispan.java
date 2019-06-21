package org.infinispan.api;

import java.util.concurrent.CompletionStage;

import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.api.reactive.KeyValueStoreConfig;

/**
 * Each infinispan is a node instance. It can be embedded or client node, depending on the access point. {@link
 * InfinispanClient} or {@link InfinispanEmbedded}
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
@Experimental
public interface Infinispan {

   /**
    * Gets the {@link KeyValueStore} by name.
    * <p>
    * If the store does not exist, creates a {@link KeyValueStore} with the default values
    *
    * @param name, name of the store
    * @return {@link KeyValueStore}
    */
   <K, V> KeyValueStore<K, V> getKeyValueStore(String name);

   /**
    * Gets the {@link KeyValueStore} by name.
    * <p>
    * If the store does not exist, creates a {@link KeyValueStore} with the given config
    *
    * @param name, name of the store
    * @return {@link KeyValueStore}
    */
   <K, V> KeyValueStore<K, V> getKeyValueStore(String name, KeyValueStoreConfig config);

   /**
    * Stops Infinispan
    *
    * @return {@link CompletionStage}
    */
   CompletionStage<Void> stop();
}
