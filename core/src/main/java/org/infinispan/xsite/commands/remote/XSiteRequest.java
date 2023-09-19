package org.infinispan.xsite.commands.remote;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletionStage;

import org.infinispan.factories.GlobalComponentRegistry;

/**
 * It represents a cross-site request.
 *
 * @since 15.0
 */
public interface XSiteRequest<T> {

   /**
    * This method is invoked by the receiver node.
    * <p>
    * The {@link GlobalComponentRegistry} gives access to every component managed by this cache manager.
    *
    * @param origin   The sender site.
    * @param registry The {@link GlobalComponentRegistry}.
    */
   CompletionStage<T> invokeInLocalSite(String origin, GlobalComponentRegistry registry);

   /**
    * Used by marshallers to convert this command into an id for streaming.
    *
    * @return the method id of this command.
    */
   byte getCommandId();

   /**
    * Writes this instance to the {@link ObjectOutput}.
    *
    * @param output the stream.
    * @throws IOException if an error occurred during the I/O.
    */
   default void writeTo(ObjectOutput output) throws IOException {
      // no-op
   }

   /**
    * Reads this instance from the stream written by {@link #writeTo(ObjectOutput)}.
    *
    * @param input the stream to read.
    * @throws IOException            if an error occurred during the I/O.
    * @throws ClassNotFoundException if it tries to load an undefined class.
    */
   default XSiteRequest<T> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      return this;
   }

}
