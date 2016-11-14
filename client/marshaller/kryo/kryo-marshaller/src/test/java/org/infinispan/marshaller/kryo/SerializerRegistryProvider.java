package org.infinispan.marshaller.kryo;

import org.infinispan.marshaller.test.User;

import com.esotericsoftware.kryo.Kryo;

import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
public class SerializerRegistryProvider implements SerializerRegistryService {
   @Override
   public void register(Kryo kryo) {
      kryo.register(User.class, new UserSerializer());
      UnmodifiableCollectionsSerializer.registerSerializers(kryo);
   }
}
