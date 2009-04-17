package org.infinispan.marshall.jboss.externalizers;

import org.infinispan.remoting.responses.ExceptionResponse;
import org.jboss.marshalling.Creator;
import org.jboss.marshalling.Externalizer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Externalizes an ExceptionResponse
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ExceptionResponseExternalizer implements Externalizer {
   public void writeExternal(Object o, ObjectOutput objectOutput) throws IOException {
      ExceptionResponse er = (ExceptionResponse) o;
      objectOutput.writeObject(er.getException());
   }

   public Object createExternal(Class<?> aClass, ObjectInput objectInput, Creator creator) throws IOException, ClassNotFoundException {
      return new ExceptionResponse();
   }

   public void readExternal(Object o, ObjectInput objectInput) throws IOException, ClassNotFoundException {
      ExceptionResponse er = (ExceptionResponse) o;
      er.setException((Exception) objectInput.readObject());
   }
}
