package org.infinispan.interceptors.impl;

import static org.infinispan.metadata.impl.PrivateMetadata.getBuilder;
import static org.infinispan.util.IracUtils.setIracMetadata;

import java.util.stream.Stream;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.Ownership;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.logging.LogSupplier;

/**
 * A {@link DDAsyncInterceptor} with common code for all the IRAC related interceptors.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public abstract class AbstractIracLocalSiteInterceptor extends DDAsyncInterceptor implements LogSupplier {

   @Inject ClusteringDependentLogic clusteringDependentLogic;
   @Inject IracVersionGenerator iracVersionGenerator;
   @Inject KeyPartitioner keyPartitioner;

   @Override
   public Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) throws Throwable {
      // Expiration isn't supported yet for xsite and lifespan doesn't need to be sent across sites
      return invokeNext(ctx, command);
   }

   protected static boolean isNormalWriteCommand(WriteCommand command) {
      return !command.hasAnyFlag(FlagBitSets.IRAC_UPDATE);
   }

   protected static boolean isIracState(FlagAffectedCommand command) {
      return command.hasAnyFlag(FlagBitSets.IRAC_STATE);
   }

   static LocalTxInvocationContext asLocalTxInvocationContext(InvocationContext ctx) {
      assert ctx.isOriginLocal();
      assert ctx.isInTxScope();
      return (LocalTxInvocationContext) ctx;
   }

   static RemoteTxInvocationContext asRemoteTxInvocationContext(InvocationContext ctx) {
      assert !ctx.isOriginLocal();
      assert ctx.isInTxScope();
      return (RemoteTxInvocationContext) ctx;
   }

   static void updateCommandMetadata(Object key, WriteCommand command, IracMetadata iracMetadata) {
      PrivateMetadata interMetadata = getBuilder(command.getInternalMetadata(key))
            .iracMetadata(iracMetadata)
            .build();
      command.setInternalMetadata(key, interMetadata);
   }

   protected Ownership getOwnership(int segment) {
      return getDistributionInfo(segment).writeOwnership();
   }

   protected DistributionInfo getDistributionInfo(int segment) {
      return getCacheTopology().getSegmentDistribution(segment);
   }

   protected boolean isWriteOwner(StreamData data) {
      return getDistributionInfo(data.segment).isWriteOwner();
   }

   protected boolean isPrimaryOwner(StreamData data) {
      return getDistributionInfo(data.segment).isPrimary();
   }

   protected LocalizedCacheTopology getCacheTopology() {
      return clusteringDependentLogic.getCacheTopology();
   }

   protected int getSegment(WriteCommand command, Object key) {
      return SegmentSpecificCommand.extractSegment(command, key, keyPartitioner);
   }

   protected void setMetadataToCacheEntry(CacheEntry<?, ?> entry, IracMetadata metadata) {
      if (entry.isEvicted()) {
         if (isTraceEnabled()) {
            getLog().tracef("[IRAC] Ignoring evict key: %s", entry.getKey());
         }
         return;
      }
      setIracMetadata(entry, metadata, iracVersionGenerator, this);
   }

   protected Stream<StreamData> streamKeysFromModifications(WriteCommand[] mods) {
      return streamKeysFromModifications(Stream.of(mods));
   }

   protected Stream<StreamData> streamKeysFromModifications(Stream<WriteCommand> modsStream) {
      return modsStream.filter(AbstractIracLocalSiteInterceptor::isNormalWriteCommand)
            .flatMap(this::streamKeysFromCommand);
   }

   protected Stream<StreamData> streamKeysFromCommand(WriteCommand command) {
      return command.getAffectedKeys().stream().map(key -> new StreamData(key, command, getSegment(command, key)));
   }

   static class StreamData {
      final Object key;
      final WriteCommand command;
      final int segment;


      public StreamData(Object key, WriteCommand command, int segment) {
         this.key = key;
         this.command = command;
         this.segment = segment;
      }

      @Override
      public String toString() {
         return "StreamData{" +
               "key=" + key +
               ", command=" + command +
               ", segment=" + segment +
               '}';
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) {
            return true;
         }
         if (o == null || getClass() != o.getClass()) {
            return false;
         }

         StreamData data = (StreamData) o;

         return segment == data.segment && key.equals(data.key);
      }

      @Override
      public int hashCode() {
         int result = key.hashCode();
         result = 31 * result + segment;
         return result;
      }
   }
}
