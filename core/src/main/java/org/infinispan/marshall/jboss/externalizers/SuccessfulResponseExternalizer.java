package org.infinispan.marshall.jboss.externalizers;

import org.infinispan.remoting.responses.SuccessfulResponse;
import org.jboss.marshalling.Creator;
import org.jboss.marshalling.Externalizer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Externalizes a SuccessfulResponse
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class SuccessfulResponseExternalizer implements Externalizer {
   public void writeExternal(Object o, ObjectOutput objectOutput) throws IOException {
      SuccessfulResponse sr = (SuccessfulResponse) o;
      objectOutput.writeObject(sr.getResponseValue());
   }

   public Object createExternal(Class<?> aClass, ObjectInput objectInput, Creator creator) throws IOException, ClassNotFoundException {
      return new SuccessfulResponse();
   }

   public void readExternal(Object o, ObjectInput objectInput) throws IOException, ClassNotFoundException {
      SuccessfulResponse sr = (SuccessfulResponse) o;
      sr.setResponseValue(objectInput.readObject());
   }
}
