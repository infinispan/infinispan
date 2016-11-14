package org.infinispan.marshaller.protostuff;

/**
 * An optional service which can be utilised to register custom protostuff schemas when executing in compatibility mode.
 *
 * N.B. This can also be used on the client, however it is not necessary as you can also register custom schemas in client
 * code, as long as the registration takes place before any Objects are marshalled/unmarshalled.
 *
 * @author Ryan Emerson
 * @since 9.0
 */
public interface SchemaRegistryService {
   /**
    * This method should be where all Custom schemas are registered.
    */
   void register();
}
