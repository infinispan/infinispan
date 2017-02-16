package org.infinispan.marshall;

import java.io.IOException;
import java.io.ObjectOutput;

import org.infinispan.atomic.DeltaAware;
import org.infinispan.commons.marshall.DelegatingObjectOutput;

/**
 * An {@link ObjectOutput} delegator that is aware of {@link DeltaAware}.
 * <p>
 * When an instance of {@link DeltaAware} is written, only the {@link DeltaAware#delta()} are written to the underline
 * {@link ObjectOutput}.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public class DeltaAwareObjectOutput extends DelegatingObjectOutput {

   public DeltaAwareObjectOutput(ObjectOutput objectOutput) {
      super(objectOutput);
   }

   @Override
   public void writeObject(Object obj) throws IOException {
      if (obj instanceof DeltaAware) {
         objectOutput.writeObject(((DeltaAware) obj).delta());
      } else {
         objectOutput.writeObject(obj);
      }
   }
}
