package org.infinispan.remoting.responses;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

/**
 * A successful response
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class SuccessfulResponse extends ValidResponse {

   private Object responseValue;

   public SuccessfulResponse() {
   }

   public SuccessfulResponse(Object responseValue) {
      this.responseValue = responseValue;
   }

   public boolean isSuccessful() {
      return true;
   }

   public Object getResponseValue() {
      return responseValue;
   }

   public void setResponseValue(Object responseValue) {
      this.responseValue = responseValue;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SuccessfulResponse that = (SuccessfulResponse) o;

      if (responseValue != null ? !responseValue.equals(that.responseValue) : that.responseValue != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return responseValue != null ? responseValue.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "SuccessfulResponse{" +
            "responseValue=" + responseValue +
            "} ";
   }

   public static class Externalizer extends AbstractExternalizer<SuccessfulResponse> {
      @Override
      public void writeObject(ObjectOutput output, SuccessfulResponse response) throws IOException {
         output.writeObject(response.responseValue);
      }

      @Override
      public SuccessfulResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new SuccessfulResponse(input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.SUCCESSFUL_RESPONSE;
      }

      @Override
      public Set<Class<? extends SuccessfulResponse>> getTypeClasses() {
         return Util.<Class<? extends SuccessfulResponse>>asSet(SuccessfulResponse.class);
      }
   }

}
