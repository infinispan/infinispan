package org.infinispan.marshall.exts;

import org.infinispan.marshall.AbstractExternalizer;

import java.io.IOException;
import java.io.ObjectOutput;

/**
 * An externalizer that writes no state. It simply marshalls class information.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public abstract class NoStateExternalizer<T> extends AbstractExternalizer<T> {

   @Override
   public void writeObject(ObjectOutput output, T object) throws IOException {
      // The instance has no state, so no-op.
   }

}
