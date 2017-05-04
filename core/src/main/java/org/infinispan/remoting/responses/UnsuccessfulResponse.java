package org.infinispan.remoting.responses;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

/**
 * An unsuccessful response
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class UnsuccessfulResponse extends ValidResponse {
   public static final UnsuccessfulResponse EMPTY = new UnsuccessfulResponse(null);
   private final Object responseValue;

   private UnsuccessfulResponse(Object value) {
      this.responseValue = value;
   }

   public static UnsuccessfulResponse create(Object value) {
      return value == null ? EMPTY : new UnsuccessfulResponse(value);
   }

   @Override
   public boolean isSuccessful() {
      return false;
   }

   public Object getResponseValue() {
      return responseValue;
   }

   @Override
   public String toString() {
      return "UnsuccessfulResponse{responseValue=" + Util.toStr(responseValue) + "} ";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      UnsuccessfulResponse that = (UnsuccessfulResponse) o;

      if (responseValue != null ? !responseValue.equals(that.responseValue) : that.responseValue != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return responseValue != null ? responseValue.hashCode() : 0;
   }

   public static class Externalizer extends AbstractExternalizer<UnsuccessfulResponse> {
      @Override
      public void writeObject(ObjectOutput output, UnsuccessfulResponse response) throws IOException {
         output.writeObject(response.getResponseValue());
      }

      @Override
      public UnsuccessfulResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return create(input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.UNSUCCESSFUL_RESPONSE;
      }

      @Override
      public Set<Class<? extends UnsuccessfulResponse>> getTypeClasses() {
         return Util.<Class<? extends UnsuccessfulResponse>>asSet(UnsuccessfulResponse.class);
      }
   }
}
