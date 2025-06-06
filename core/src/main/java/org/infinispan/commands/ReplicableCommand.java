package org.infinispan.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.upgrade.VersionAware;

/**
 * The core of the command-based cache framework.  Commands correspond to specific areas of functionality in the cache,
 * and can be replicated using the {@link org.infinispan.remoting.rpc.RpcManager}
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 * @since 4.0
 */
public interface ReplicableCommand extends TracedCommand, VersionAware {

   /**
    * Used by marshallers to convert this command into an id for streaming.
    *
    * @return the method id of this command.  This is compatible with pre-2.2.0 MethodCall ids.
    * @deprecated since 16.0 as this is no longer required by the Protostream based marshaller. Implementations of this
    * method are always ignored.
    */
   @Deprecated(since = "16.0", forRemoval = true)
   default byte getCommandId() {
      return 0;
   }

   /**
    * If true, a return value will be provided when performed remotely.  Otherwise, a remote {@link
    * org.infinispan.remoting.responses.ResponseGenerator} may choose to simply return null to save on marshalling
    * costs.
    *
    * @return true or false
    */
   boolean isReturnValueExpected();

   /**
    * If true, a return value will be marshalled as a {@link org.infinispan.remoting.responses.SuccessfulResponse},
    * otherwise it will be marshalled as a {@link org.infinispan.remoting.responses.UnsuccessfulResponse}.
    */
   default boolean isSuccessful() {
      return true;
   }

   default boolean logThrowable(Throwable t) {
      return true;
   }

   /**
    * Writes this instance to the {@link ObjectOutput}.
    *
    * @param output the stream.
    * @throws IOException if an error occurred during the I/O.
    * @deprecated since 16.0 command objects should be marshalled directly by ProtoStream
    */
   @Deprecated(forRemoval = true, since = "16.0")
   default void writeTo(ObjectOutput output) throws IOException {
      // no-op
   }

   /**
    * Reads this instance from the stream written by {@link #writeTo(ObjectOutput)}.
    *
    * @param input the stream to read.
    * @throws IOException            if an error occurred during the I/O.
    * @throws ClassNotFoundException if it tries to load an undefined class.
    * @deprecated since 16.0 command objects should be marshalled directly by ProtoStream
    */
   @Deprecated(forRemoval = true, since = "16.0")
   default void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      // no-op
   }

   /**
    * Sets the sender's {@link Address}.
    * <p>
    * By default, it doesn't set anything. Implement this method if the sender's {@link Address} is needed.
    *
    * @param origin the sender's {@link Address}
    */
   default void setOrigin(Address origin) {
      //no-op by default
   }

   /**
    * Returns a {@link NodeVersion} representing the Infinispan version in which this command was added. This value
    * is used to ensure that when the cluster contains different Infinispan versions, only commands compatible with the
    * oldest version are transmitted.
    * <p>
    * Abstract classes should not implement this method as the version should be specific to an individual implementation.
    * Similarly, implementations which extend another {@link ReplicableCommand} should always override this method.
    *
    * @return a {@link NodeVersion} corresponding to the Infinispan version this command was added.
    */
   @Override
   NodeVersion supportedSince();
}
