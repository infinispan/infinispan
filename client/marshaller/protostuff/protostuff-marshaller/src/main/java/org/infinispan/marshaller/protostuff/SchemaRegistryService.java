package org.infinispan.marshaller.protostuff;

/**
 * An optional service which can be utilised to register custom protostuff schemas when executing storing data deserialized.
 *
 * N.B. This can also be used on the client, however it is not necessary as you can also register custom schemas in client
 * code, as long as the registration takes place before any Objects are marshalled/unmarshalled.
 *
 * @author Ryan Emerson
 * @since 9.0
 * @deprecated since 12.0 without a direct replacement, will be removed in 15.0 ISPN-12152
 */
@Deprecated(forRemoval = true)
public interface SchemaRegistryService {
   /**
    * This method should be where all Custom schemas are registered.
    */
   void register();
}
