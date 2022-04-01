package org.infinispan.commands.irac;

import static org.infinispan.commons.marshall.MarshallUtil.marshallCollection;
import static org.infinispan.commons.marshall.MarshallUtil.unmarshallCollection;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.irac.IracManagerKeyInfo;
import org.infinispan.xsite.irac.IracManagerKeyInfoImpl;

/**
 * Sends a cleanup request from the primary owner to the backup owners.
 * <p>
 * Sent after a successful update of all remote sites.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class IracCleanupKeysCommand implements CacheRpcCommand {

   public static final byte COMMAND_ID = 122;

   private ByteString cacheName;
   private Collection<? extends IracManagerKeyInfo> cleanup;

   @SuppressWarnings("unused")
   public IracCleanupKeysCommand() {
   }

   public IracCleanupKeysCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracCleanupKeysCommand(ByteString cacheName, Collection<? extends IracManagerKeyInfo> cleanup) {
      this.cacheName = cacheName;
      this.cleanup = cleanup;
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public CompletableFuture<Object> invokeAsync(ComponentRegistry componentRegistry) {
      IracManager manager = componentRegistry.getIracManager().running();
      cleanup.forEach(manager::removeState);
      return CompletableFutures.completedNull();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      marshallCollection(cleanup, output, IracManagerKeyInfoImpl::writeTo);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      cleanup = unmarshallCollection(input, ArrayList::new, IracManagerKeyInfoImpl::readFrom);
   }

   @Override
   public Address getOrigin() {
      //not needed
      return null;
   }

   @Override
   public void setOrigin(Address origin) {
      //no-op
   }

   @Override
   public String toString() {
      return "IracCleanupKeyCommand{" +
            "cacheName=" + cacheName +
            ", cleanup=" + Util.toStr(cleanup) +
            '}';
   }
}
