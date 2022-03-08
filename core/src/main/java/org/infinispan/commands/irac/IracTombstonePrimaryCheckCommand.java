package org.infinispan.commands.irac;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.container.versioning.irac.IracTombstoneInfo;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.irac.IracManager;

/**
 * A {@link CacheRpcCommand} to check if one or more tombstones are still valid.
 * <p>
 * Periodically, the backup owner send this command for the tombstones they have stored if the key is not present in
 * {@link IracManager}.
 *
 * @since 14.0
 */
public class IracTombstonePrimaryCheckCommand implements CacheRpcCommand {

   public static final byte COMMAND_ID = 47;

   private ByteString cacheName;
   private Collection<IracTombstoneInfo> tombstoneToCheck;

   @SuppressWarnings("unused")
   public IracTombstonePrimaryCheckCommand() {
   }

   public IracTombstonePrimaryCheckCommand(ByteString cacheName) {
      this.cacheName = cacheName;
   }

   public IracTombstonePrimaryCheckCommand(ByteString cacheName, Collection<IracTombstoneInfo> tombstoneToCheck) {
      this.cacheName = cacheName;
      this.tombstoneToCheck = tombstoneToCheck;
   }

   @Override
   public CompletionStage<Void> invokeAsync(ComponentRegistry registry) {
      registry.getIracTombstoneManager().running().checkStaleTombstone(tombstoneToCheck);
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
      MarshallUtil.marshallCollection(tombstoneToCheck, output, IracTombstoneInfo::writeTo);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      tombstoneToCheck = MarshallUtil.unmarshallCollection(input, ArrayList::new, IracTombstoneInfo::readFrom);
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public Address getOrigin() {
      //no-op
      return null;
   }

   @Override
   public void setOrigin(Address origin) {
      //no-op
   }

   @Override
   public String toString() {
      return "IracTombstonePrimaryCheckCommand{" +
            "cacheName=" + cacheName +
            ", tombstoneToCheck=" + tombstoneToCheck +
            '}';
   }

   public Collection<IracTombstoneInfo> getTombstoneToCheck() {
      return tombstoneToCheck;
   }
}
