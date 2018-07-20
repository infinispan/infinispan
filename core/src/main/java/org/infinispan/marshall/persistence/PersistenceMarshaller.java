package org.infinispan.marshall.persistence;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamAwareMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;

/**
 * The marshaller that is responsible serializaing/desearilizing objects which are to be persisted.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public interface PersistenceMarshaller extends Marshaller, StreamAwareMarshaller {

   /**
    * Registers the schemas and marshallers defined by the provided {@link SerializationContextInitializer} with the
    * {@link PersistenceMarshaller}'s {@link SerializationContext}.
    *
    * @param initializer whose schemas and marshallers' will be registered with the {@link PersistenceMarshaller} {@link
    *                    SerializationContext}
    */
   void register(SerializationContextInitializer initializer);
}
