package org.infinispan.remoting.responses;

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
}
