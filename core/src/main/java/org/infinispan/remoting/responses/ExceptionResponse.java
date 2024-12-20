package org.infinispan.remoting.responses;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableThrowable;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A response that encapsulates an exception
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.EXCEPTION_RESPONSE)
public class ExceptionResponse extends InvalidResponse {

   @ProtoField(number = 1)
   MarshallableThrowable exception;

   @ProtoFactory
   ExceptionResponse(MarshallableThrowable exception) {
      this.exception = exception;
   }

   public ExceptionResponse(Exception exception) {
      setException(exception);
   }

   public Exception getException() {
      return (Exception) exception.get();
   }

   public void setException(Exception exception) {
      this.exception = MarshallableThrowable.create(exception);
   }

   @Override
   public String toString() {
      return "ExceptionResponse(" + exception + ")";
   }
}
