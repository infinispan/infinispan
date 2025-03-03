package org.infinispan.commands.write;

import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
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
@ProtoTypeId(ProtoStreamTypeIds.IRAC_PUT_KEY_VALUE_COMMAND)
public class IracPutKeyValueCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {

   private Object value;
   private Metadata metadata;
   private PrivateMetadata privateMetadata;
   private transient boolean successful = true;
   private boolean expiration;

   public IracPutKeyValueCommand(Object key, int segment, CommandInvocationId commandInvocationId, Object value,
         Metadata metadata, PrivateMetadata privateMetadata) {
      super(key, segment, FlagBitSets.IRAC_UPDATE, commandInvocationId);
      assert privateMetadata != null;
      this.value = value;
      this.metadata = metadata;
      this.privateMetadata = privateMetadata;
   }

   @ProtoFactory
   IracPutKeyValueCommand(MarshallableObject<?> wrappedKey, long flagsWithoutRemote, int topologyId, int segment,
                          CommandInvocationId commandInvocationId, MarshallableObject<Object> wrappedValue,
                          MarshallableObject<Metadata> wrappedMetadata, boolean expiration, PrivateMetadata internalMetadata) {
      super(wrappedKey, FlagBitSets.IRAC_UPDATE, topologyId, segment, commandInvocationId);
      this.value = MarshallableObject.unwrap(wrappedValue);
      this.metadata = MarshallableObject.unwrap(wrappedMetadata);
      this.expiration = expiration;
      this.privateMetadata = internalMetadata;
   }

   @ProtoField(6)
   MarshallableObject<Object> getWrappedValue() {
      return MarshallableObject.create(value);
   }

   @ProtoField(7)
   MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
   }

   @ProtoField(8)
   boolean getExpiration() {
      return expiration;
   }

   @Override
   @ProtoField(9)
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
