package org.infinispan.marshall.protostream.impl;

import org.infinispan.protostream.WrappedMessage;

/**
 * A utility class for common actions related to ProtoStream {@link WrappedMessage}.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
public class WrappedMessages {

   /**
    * @param o the object to be wrapped
    * @return null if o is null, otherwise return a {@link WrappedMessage} containing o.
    */
   public static WrappedMessage orElseNull(Object o) {
      return o == null ? null : new WrappedMessage(o);
   }

   @SuppressWarnings("unchecked")
   public static <T> T unwrap(WrappedMessage wrappedMessage) {
      return wrappedMessage == null ? null : (T) wrappedMessage.getValue();
   }
}
