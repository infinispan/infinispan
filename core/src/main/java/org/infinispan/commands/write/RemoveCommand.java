package org.infinispan.commands.write;

import static org.infinispan.commons.util.Util.toStr;

import java.util.Objects;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.MetadataAwareCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;


/**
 * @author Mircea.Markus@jboss.com
 * @author <a href="mailto:galder.zamarreno@jboss.com">Galder Zamarreno</a>
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.REMOVE_COMMAND)
public class RemoveCommand extends AbstractDataWriteCommand implements MetadataAwareCommand {
   public static final byte COMMAND_ID = 10;
   private boolean returnEntry = false;
   private transient boolean nonExistent = false;
   protected transient boolean successful = true;

   protected Metadata metadata;
   protected ValueMatcher valueMatcher;
   private PrivateMetadata internalMetadata;

   /**
    * When not null, value indicates that the entry should only be removed if the key is mapped to this value.
    * When null, the entry should be removed regardless of what value it is mapped to.
    */
   protected Object value;

   public RemoveCommand(Object key, Object value, boolean returnEntry, int segment, long flagsBitSet,
                        CommandInvocationId commandInvocationId) {
      super(key, segment, flagsBitSet, commandInvocationId);
      setValue(value);
      this.valueMatcher = value != null ? ValueMatcher.MATCH_EXPECTED : ValueMatcher.MATCH_ALWAYS;
      this.returnEntry = returnEntry;
   }

   @ProtoFactory
   RemoveCommand(MarshallableObject<?> wrappedKey, long flagsWithoutRemote, int topologyId, int segment,
                 CommandInvocationId commandInvocationId, MarshallableObject<?> wrappedValue,
                 MarshallableObject<Metadata> wrappedMetadata, ValueMatcher valueMatcher,
                 PrivateMetadata internalMetadata, boolean returnEntryNecessary) {
      super(wrappedKey, flagsWithoutRemote, topologyId, segment, commandInvocationId);
      this.value = MarshallableObject.unwrap(wrappedValue);
      this.metadata = MarshallableObject.unwrap(wrappedMetadata);
      this.valueMatcher = valueMatcher;
      this.internalMetadata = internalMetadata;
      this.returnEntry = returnEntryNecessary;
   }

   @ProtoField(number = 6, name = "value")
   protected MarshallableObject<?> getWrappedValue() {
      return MarshallableObject.create(value);
   }

   @ProtoField(number = 7, name = "metadata")
   protected MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
   }

   @Override
   @ProtoField(8)
   public ValueMatcher getValueMatcher() {
      return valueMatcher;
   }

   @ProtoField(9)
   public PrivateMetadata getInternalMetadata() {
      return internalMetadata;
   }

   public void setInternalMetadata(PrivateMetadata internalMetadata) {
      this.internalMetadata = internalMetadata;
   }

   @ProtoField(10)
   public boolean isReturnEntryNecessary() {
      return returnEntry;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitRemoveCommand(ctx, this);
   }

   @Override
   public boolean shouldReplicate(InvocationContext ctx, boolean requireReplicateIfRemote) {
      if (!isSuccessful()) {
         return false;
      }
      // XSITE backup should always replicate remove command
      // If skip cache load is set we don't know if the store had a null value for remove so we have to replicate still
      // Also if this is a backup write then we can't skip replication to stores
      // Also if the caller says we must replicte on remote, make sure we are local
      return (!nonExistent || hasAnyFlag(FlagBitSets.SKIP_XSITE_BACKUP |
            FlagBitSets.SKIP_CACHE_LOAD) || (requireReplicateIfRemote && (ctx == null || !ctx.isOriginLocal())));
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public boolean equals(Object o) {
      if (!super.equals(o)) {
         return false;
      }

      RemoveCommand that = (RemoveCommand) o;

      return Objects.equals(value, that.value);
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
   }


   @Override
   public String toString() {
      return new StringBuilder()
         .append("RemoveCommand{key=")
         .append(toStr(key))
         .append(", value=").append(toStr(value))
         .append(", returnEntry=").append(returnEntry)
         .append(", metadata=").append(metadata)
         .append(", internalMetadata=").append(internalMetadata)
         .append(", flags=").append(printFlags())
         .append(", commandInvocationId=").append(CommandInvocationId.show(commandInvocationId))
         .append(", valueMatcher=").append(valueMatcher)
         .append(", topologyId=").append(getTopologyId())
         .append("}")
         .toString();
   }

   @Override
   public boolean isSuccessful() {
      return successful;
   }

   @Override
   public boolean isConditional() {
      return value != null;
   }

   public void nonExistant() {
      nonExistent = true;
   }

   @Override
   public void setValueMatcher(ValueMatcher valueMatcher) {
      this.valueMatcher = valueMatcher;
   }

   @Override
   public void fail() {
      successful = false;
   }

   @Override
   public LoadType loadType() {
      return isConditional() || !hasAnyFlag(FlagBitSets.IGNORE_RETURN_VALUES) ? LoadType.PRIMARY : LoadType.DONT_LOAD;
   }

   public Object getValue() {
      return value;
   }

   public void setValue(Object value) {
      this.value = value;
   }

   @Override
   public final boolean isReturnValueExpected() {
      // IGNORE_RETURN_VALUES ignored for conditional remove
      return isConditional() || super.isReturnValueExpected();
   }
}
