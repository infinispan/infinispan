package org.infinispan.server.websocket.logging;

import org.infinispan.server.websocket.json.JsonConversionException;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the websocket server. For this module, message ids
 * ranging from 13001 to 14000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @Message(value = "Could not convert from String to Json: %s", id = 13001)
   JsonConversionException unableToConvertFromStringToJson(String json, @Cause Throwable e);

   @Message(value = "Could not convert from Object to Json: %s", id = 13002)
   JsonConversionException unableToConvertFromObjectToJson(Object o, @Cause Throwable e);

   @Message(value = "Error while converting from Json to String", id = 13003)
   IllegalStateException unableToConvertFromJsonToString(@Cause Throwable t);

   @Message(value = "Unexpected exception while closing Websockets script stream.", id = 13004)
   IllegalStateException unableToCloseWebSocketsStream(@Cause Throwable t);

   @Message(value = "Unexpected exception while sending Websockets script to client.", id = 13005)
   IllegalStateException unableToSendWebSocketsScriptToTheClient(@Cause Throwable t);

   @Message(value = "Complex object graphs not yet supported!! Cannot cache value: %s", id = 13006)
   UnsupportedOperationException complexGraphObjectAreNotYetSupported(String json);

   @Message(value = "null '%s' arg in method or constructor call.", id = 13007)
   IllegalArgumentException invalidNullArgument(String fieldName);

   @Message(value = "Could not get fields from object", id = 13008)
   IllegalStateException unableToGetFieldsFromObject(@Cause Throwable t);


}
