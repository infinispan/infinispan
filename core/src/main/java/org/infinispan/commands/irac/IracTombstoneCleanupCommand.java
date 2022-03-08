package org.infinispan.commands.irac;

import static org.infinispan.commons.marshall.MarshallUtil.marshallCollection;
import static org.infinispan.commons.marshall.MarshallUtil.unmarshallCollection;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.container.versioning.irac.IracTombstoneInfo;
import org.infinispan.container.versioning.irac.IracTombstoneManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * A {@link CacheRpcCommand} to clean up tombstones for IRAC algorithm.
 * <p>
 * This command is sent from the primary owners to the backup owner with the tombstone to be removed. No response is
 * expected from the backup owners.
 *
 * @since 14.0
 */
public class IracTombstoneCleanupCommand implements CacheRpcCommand {

   public static final byte COMMAND_ID = 37;

   private final ByteString cacheName;
   private Collection<IracTombstoneInfo> tombstonesToRemove;

   @SuppressWarnings("unused")
   public IracTombstoneCleanupCommand() {
      this(null, 1);
   }

   public IracTombstoneCleanupCommand(ByteString cacheName) {
      this(cacheName, 1);
   }

   public IracTombstoneCleanupCommand(ByteString cacheName, int maxCapacity) {
      this.cacheName = cacheName;
      tombstonesToRemove = new HashSet<>(maxCapacity);
   }

   @Override
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public CompletionStage<Boolean> invokeAsync(ComponentRegistry registry) {
      IracTombstoneManager tombstoneManager = registry.getIracTombstoneManager().running();
      tombstonesToRemove.forEach(tombstoneManager::removeTombstone);
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
      marshallCollection(tombstonesToRemove, output, IracTombstoneInfo::writeTo);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      tombstonesToRemove = unmarshallCollection(input, ArrayList::new, IracTombstoneInfo::readFrom);
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
      return "IracTombstoneCleanupCommand{" +
            "cacheName=" + cacheName +
            ", tombstone=" + tombstonesToRemove +
            '}';
   }

   public void add(IracTombstoneInfo tombstone) {
      tombstonesToRemove.add(tombstone);
   }

   public int size() {
      return tombstonesToRemove.size();
   }

   public boolean isEmpty() {
      return tombstonesToRemove.isEmpty();
   }

   public Collection<IracTombstoneInfo> getTombstonesToRemove() {
      return tombstonesToRemove;
   }
}
