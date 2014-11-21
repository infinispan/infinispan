package org.infinispan.remoting.responses;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Array;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

/**
 * A successful response
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class SuccessfulResponse extends ValidResponse {
   public static final SuccessfulResponse SUCCESSFUL_EMPTY_RESPONSE = new SuccessfulResponse(null);

   private final Object responseValue;

   private SuccessfulResponse(Object responseValue) {
      this.responseValue = responseValue;
   }

   public static SuccessfulResponse create(Object responseValue) {
      return responseValue == null ? SUCCESSFUL_EMPTY_RESPONSE : new SuccessfulResponse(responseValue);
   }

   @Override
   public boolean isSuccessful() {
      return true;
   }

   public Object getResponseValue() {
      return responseValue;
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
            "responseValue=" + toStr(responseValue) +
            "} ";
   }

   private String toStr(Object responseValue) {
      if (responseValue == null || !responseValue.getClass().isArray()) {
         return String.valueOf(responseValue);
      }
      int length = Array.getLength(responseValue);
      if (length == 0) return "[]";

      StringBuilder sb = new StringBuilder("[").append(Array.get(responseValue, 0));
      for (int i = 1; i < length; ++i) sb.append(", ").append(Array.get(responseValue, i));
      return sb.append("]").toString();
   }

   public static class Externalizer extends AbstractExternalizer<SuccessfulResponse> {
      @Override
      public void writeObject(ObjectOutput output, SuccessfulResponse response) throws IOException {
         if (response.responseValue == null) {
            output.writeBoolean(false);
         } else {
            output.writeBoolean(true);
            output.writeObject(response.responseValue);
         }
      }

      @Override
      public SuccessfulResponse readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         boolean nonNullResponse = input.readBoolean();
         if (nonNullResponse) {
            return new SuccessfulResponse(input.readObject());
         } else {
            return SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE;
         }
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
