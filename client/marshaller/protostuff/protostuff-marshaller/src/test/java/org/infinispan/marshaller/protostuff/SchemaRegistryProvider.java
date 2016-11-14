package org.infinispan.marshaller.protostuff;

import org.infinispan.marshaller.test.User;

import io.protostuff.runtime.RuntimeSchema;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
public class SchemaRegistryProvider implements SchemaRegistryService {
   @Override
   public void register() {
      RuntimeSchema.register(User.class, new UserSchema());
   }
}
