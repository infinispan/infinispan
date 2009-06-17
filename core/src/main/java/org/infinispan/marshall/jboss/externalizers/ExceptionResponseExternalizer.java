package org.infinispan.marshall.jboss.externalizers;

import net.jcip.annotations.Immutable;

import org.infinispan.marshall.jboss.Externalizer;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;

/**
 * Externalizes an ExceptionResponse
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Immutable
public class ExceptionResponseExternalizer implements Externalizer {

   public void writeObject(Marshaller output, Object subject) throws IOException {
      ExceptionResponse er = (ExceptionResponse) subject;
      output.writeObject(er.getException());
   }

   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      ExceptionResponse er = new ExceptionResponse();
      er.setException((Exception) input.readObject());
      return er;
   }

}
