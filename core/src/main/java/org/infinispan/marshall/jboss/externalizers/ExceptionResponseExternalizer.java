package org.infinispan.marshall.jboss.externalizers;

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
public class ExceptionResponseExternalizer implements Externalizer {
   /** The serialVersionUID */
   private static final long serialVersionUID = -8972357475889354040L;

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
