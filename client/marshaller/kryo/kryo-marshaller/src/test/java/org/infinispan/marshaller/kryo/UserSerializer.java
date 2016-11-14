package org.infinispan.marshaller.kryo;


import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.marshaller.test.User;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
class UserSerializer extends Serializer<User> {

   static final AtomicInteger writeCount = new AtomicInteger();
   static final AtomicInteger readCount = new AtomicInteger();

   public void write (Kryo kryo, Output output, User user) {
      writeCount.incrementAndGet();
      output.writeString(user.getName());
   }

   public User read (Kryo kryo, Input input, Class<User> type) {
      readCount.incrementAndGet();
      return new User(input.readString());
   }
}
