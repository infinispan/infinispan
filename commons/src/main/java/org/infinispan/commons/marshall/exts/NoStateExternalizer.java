package org.infinispan.commons.marshall.exts;

import java.io.IOException;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectOutput;

/**
 * An externalizer that writes no state. It simply marshalls class information.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public abstract class NoStateExternalizer<T> extends AbstractExternalizer<T> {

   @Override
   public void writeObject(UserObjectOutput output, T object) throws IOException {
      // The instance has no state, so no-op.
   }

}
