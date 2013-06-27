package org.infinispan.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;

/**
 * LegacyAdvancedExternalizerAdapter.
 *
 * @author Tristan Tarrant
 * @since 6.0
 * @deprecated Convert your externalizers to {@link org.infinispan.commons.marshall.AdvancedExternalizer}
 */
@Deprecated
public class LegacyAdvancedExternalizerAdapter<T> implements AdvancedExternalizer<T> {

   final org.infinispan.marshall.AdvancedExternalizer<T> delegate;

   public LegacyAdvancedExternalizerAdapter(org.infinispan.marshall.AdvancedExternalizer<T> delegate) {
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

   @Override
   public Set<Class<? extends T>> getTypeClasses() {
      return delegate.getTypeClasses();
   }

   @Override
   public Integer getId() {
      return delegate.getId();
   }

}
