package org.infinispan.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commons.marshall.Externalizer;

/**
 * LegacyExternalizerAdapter.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Deprecated
public class LegacyExternalizerAdapter<T> implements Externalizer<T> {
   final org.infinispan.marshall.Externalizer<T> delegate;

   public LegacyExternalizerAdapter(org.infinispan.marshall.Externalizer<T> delegate) {
      this.delegate = delegate;
   }

   @Override
   public void writeObject(ObjectOutput output, T object) throws IOException {
      delegate.writeObject(output, object);
   }

   @Override
   public T readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return delegate.readObject(input);
   }
}
