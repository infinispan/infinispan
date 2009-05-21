package org.infinispan.marshall.jboss.externalizers;

import net.jcip.annotations.Immutable;

import org.infinispan.marshall.jboss.Externalizer; 
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;

/**
 * Externalizes a SuccessfulResponse
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Immutable
public class SuccessfulResponseExternalizer implements Externalizer {

   public void writeObject(Marshaller output, Object subject) throws IOException {
      SuccessfulResponse sr = (SuccessfulResponse) subject;
      output.writeObject(sr.getResponseValue());      
   }

   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      SuccessfulResponse sr = new SuccessfulResponse();
      sr.setResponseValue(input.readObject());
      return sr;
   }

}
