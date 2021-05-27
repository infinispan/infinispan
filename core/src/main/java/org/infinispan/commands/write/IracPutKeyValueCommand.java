package org.infinispan.commands.write;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.xsite.spi.SiteEntry;
import org.infinispan.xsite.spi.XSiteEntryMergePolicy;

/**
 * A {@link WriteCommand} used to handle updates from the remote site (for asynchronous cross-site replication).
 * <p>
 * Asynchronous cross-site replication may originate conflicts and this command allows to change its value based on the
 * user's {@link XSiteEntryMergePolicy} installed. The value (and metadata) can change until the command reaches the end
 * of the {@link AsyncInterceptorChain}, where the {@link CallInterceptor} checks its state and updates or removes the
 * key.
 * <p>
 * Note, this command is non-transactional, even for transactional caches. This simplifies the conflict resolution.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public class IracPutKeyValueCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {

   public static final byte COMMAND_ID = 28;

   private Object value;
   private Metadata metadata;
   private PrivateMetadata privateMetadata;
   private boolean successful = true;
   private boolean expiration;

   public IracPutKeyValueCommand() {}

   public IracPutKeyValueCommand(Object key, int segment, CommandInvocationId commandInvocationId, Object value,
         Metadata metadata, PrivateMetadata privateMetadata) {
      super(key, segment, FlagBitSets.IRAC_UPDATE, commandInvocationId);
      assert privateMetadata != null;
      this.value = value;
      this.metadata = metadata;
      this.privateMetadata = privateMetadata;
   }

   @Override
   public PrivateMetadata getInternalMetadata() {
      return privateMetadata;
   }

   @Override
   public void setInternalMetadata(PrivateMetadata internalMetadata) {
      this.privateMetadata = internalMetadata;
   }

   @Override
   public PrivateMetadata getInternalMetadata(Object key) {
      assert Objects.equals(this.key, key);
      return getInternalMetadata();
   }

   @Override
   public void setInternalMetadata(Object key, PrivateMetadata internalMetadata) {
      assert Objects.equals(this.key, key);
      setInternalMetadata(internalMetadata);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

   @Override
   public boolean isConditional() {
      return false;
   }

   @Override
   public ValueMatcher getValueMatcher() {
      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      //no-op
   }

   @Override
   public void fail() {
      this.successful = false;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitIracPutKeyValueCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      //primary owner always need the previous value to check the versions.
      return LoadType.PRIMARY;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   public Object getValue() {
      return value;
   }

   /**
    * @return {@code true} if this command state is a removal operation, {@code false} otherwise.
    */
   public boolean isRemove() {
      return value == null;
   }

   /**
    * Creates the {@link SiteEntry} to be used in {@link XSiteEntryMergePolicy}.
    *
    * @param site The remote site name.
    * @return The {@link SiteEntry}.
    */
   public SiteEntry<Object> createSiteEntry(String site) {
      return new SiteEntry<>(site, value, metadata);
   }

   /**
    * Updates this command state with the result of {@link XSiteEntryMergePolicy#merge(Object, SiteEntry,
    * SiteEntry)}.
    *
    * @param siteEntry The resolved {@link SiteEntry}.
    */
   public void updateCommand(SiteEntry<Object> siteEntry) {
      this.value = siteEntry.getValue();
      setMetadata(siteEntry.getMetadata());
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      output.writeObject(key);
      output.writeObject(value);
      output.writeObject(metadata);
      CommandInvocationId.writeTo(output, commandInvocationId);
      output.writeObject(privateMetadata);
      UnsignedNumeric.writeUnsignedInt(output, segment);
      output.writeBoolean(expiration);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      key = input.readObject();
      value = input.readObject();
      metadata = (Metadata) input.readObject();
      commandInvocationId = CommandInvocationId.readFrom(input);
      privateMetadata = (PrivateMetadata) input.readObject();
      segment = UnsignedNumeric.readUnsignedInt(input);
      expiration = input.readBoolean();
      setFlagsBitSet(FlagBitSets.IRAC_UPDATE);
   }

   public boolean isExpiration() {
      return expiration;
   }

   public void setExpiration(boolean expiration) {
      this.expiration = expiration;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      if (!super.equals(o)) {
         return false;
      }
      IracPutKeyValueCommand that = (IracPutKeyValueCommand) o;
      return Objects.equals(value, that.value) &&
            Objects.equals(metadata, that.metadata) &&
            Objects.equals(privateMetadata, that.privateMetadata);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), value, metadata, privateMetadata);
   }

   @Override
   public String toString() {
      return "IracPutKeyValueCommand{" +
            "key=" + key +
            ", value=" + value +
            ", metadata=" + metadata +
            ", privateMetadata=" + privateMetadata +
            ", successful=" + successful +
            ", commandInvocationId=" + commandInvocationId +
            '}';
   }
}
