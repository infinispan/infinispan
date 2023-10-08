package org.infinispan.commands;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.remoting.transport.Address;

/**
 * The core of the command-based cache framework.  Commands correspond to specific areas of functionality in the cache,
 * and can be replicated using the {@link org.infinispan.remoting.rpc.RpcManager}
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 * @since 4.0
 */
public interface ReplicableCommand extends TracedCommand {

   /**
    * Invoke the command asynchronously.
    *
    * @since 9.0
    * @deprecated since 11.0, please use {@link org.infinispan.commands.remote.CacheRpcCommand#invokeAsync(ComponentRegistry)}
    * or {@link GlobalRpcCommand#invokeAsync(GlobalComponentRegistry)} instead.
    */
   @Deprecated(forRemoval=true, since = "11.0")
   default CompletableFuture<Object> invokeAsync() throws Throwable {
      return CompletableFuture.completedFuture(invoke());
   }

   /**
    * Invoke the command synchronously.
    *
    * @since 9.0
    * @deprecated since 11.0, please use {@link org.infinispan.commands.remote.CacheRpcCommand#invokeAsync(ComponentRegistry)}
    * or {@link GlobalRpcCommand#invokeAsync(GlobalComponentRegistry)} instead.
    */
   @Deprecated(forRemoval=true, since = "11.0")
   default Object invoke() throws Throwable {
      try {
         return invokeAsync().join();
      } catch (CompletionException e) {
         throw CompletableFutures.extractException(e);
      }
   }

   /**
    * Used by marshallers to convert this command into an id for streaming.
    *
    * @return the method id of this command.  This is compatible with pre-2.2.0 MethodCall ids.
    */
   // TODO do we just ignore this? Not possible to use ProtoStreamTypeIDs as it's a byte
   byte getCommandId();

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

   /**
    * If true, the command is processed asynchronously in a thread provided by an Infinispan thread pool. Otherwise,
    * the command is processed directly in the JGroups thread.
    * <p/>
    * This feature allows to avoid keep a JGroups thread busy that can originate discard of messages and
    * retransmissions. So, the commands that can block (waiting for some state, acquiring locks, etc.) should return
    * true.
    *
    * @return {@code true} if the command can block/wait, {@code false} otherwise
    * @deprecated since 11.0 - All commands will be required to be non blocking!
    */
   @Deprecated(forRemoval=true)
   default boolean canBlock() {
      return false;
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
}
