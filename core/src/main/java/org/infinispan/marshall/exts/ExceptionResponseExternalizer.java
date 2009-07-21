package org.infinispan.marshall.exts;

import net.jcip.annotations.Immutable;

import org.infinispan.marshall.Externalizer;
import org.infinispan.remoting.responses.ExceptionResponse;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Externalizes an ExceptionResponse
 *
 * @author Manik Surtani
 * @since 4.0
 * @deprecated Externalizer implementation now within {@link ExceptionResponse}
 */
@Immutable
public class ExceptionResponseExternalizer implements Externalizer {

   public void writeObject(ObjectOutput output, Object subject) throws IOException {
      ExceptionResponse er = (ExceptionResponse) subject;
      output.writeObject(er.getException());
   }

   public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      ExceptionResponse er = new ExceptionResponse();
      er.setException((Exception) input.readObject());
      return er;
   }

}
