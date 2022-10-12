package org.infinispan.interceptors.impl;

import static org.infinispan.metadata.impl.PrivateMetadata.getBuilder;
import static org.infinispan.util.IracUtils.setIracMetadata;

import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.stream.Stream;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracTombstoneManager;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.InvocationFinallyAction;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.CacheTopologyUtil;
import org.infinispan.util.IracUtils;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.LogSupplier;

/**
 * A {@link DDAsyncInterceptor} with common code for all the IRAC related interceptors.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public abstract class AbstractIracLocalSiteInterceptor extends DDAsyncInterceptor implements LogSupplier {

   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject ClusteringDependentLogic clusteringDependentLogic;
   @Inject IracVersionGenerator iracVersionGenerator;
   @Inject IracTombstoneManager iracTombstoneManager;
   @Inject KeyPartitioner keyPartitioner;

   private final InvocationFinallyAction<DataWriteCommand> afterWriteCommand = this::handleNonTxDataWriteCommand;

   @Override
   public final Object visitRemoveExpiredCommand(InvocationContext ctx, RemoveExpiredCommand command) {
      return visitNonTxDataWriteCommand(ctx, command);
   }

   @Override
   public final boolean isTraceEnabled() {
      return log.isTraceEnabled();
   }

   @Override
   public final Log getLog() {
      return log;
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

   protected void setMetadataToCacheEntry(CacheEntry<?, ?> entry, int segment, IracMetadata metadata) {
      if (entry.isEvicted()) {
         if (log.isTraceEnabled()) {
            log.tracef("[IRAC] Ignoring evict key: %s", entry.getKey());
         }
         return;
      }
      setIracMetadata(entry, segment, metadata, iracTombstoneManager, this);
   }

   protected Stream<StreamData> streamKeysFromModifications(Stream<WriteCommand> modsStream) {
      return modsStream.filter(AbstractIracLocalSiteInterceptor::isNormalWriteCommand)
            .flatMap(this::streamKeysFromCommand);
   }

   protected Stream<StreamData> streamKeysFromCommand(WriteCommand command) {
      return command.getAffectedKeys().stream().map(key -> new StreamData(key, command, getSegment(command, key)));
   }

   protected boolean skipEntryCommit(InvocationContext ctx, WriteCommand command, Object key) {
      LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      switch (cacheTopology.getSegmentDistribution(getSegment(command, key)).writeOwnership()) {
         case NON_OWNER:
            //not a write owner, we do nothing
            return true;
         case BACKUP:
            //if it is local, we do nothing.
            //the update happens in the remote context after the primary validated the write
            return ctx.isOriginLocal();
      }
      return false;
   }

   protected Object visitNonTxDataWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      final Object key = command.getKey();
      if (isIracState(command)) { //all the state transfer/preload is done via put commands.
         setMetadataToCacheEntry(ctx.lookupEntry(key), command.getSegment(), command.getInternalMetadata(key).iracMetadata());
         return invokeNext(ctx, command);
      }
      if (command.hasAnyFlag(FlagBitSets.IRAC_UPDATE)) {
         return invokeNext(ctx, command);
      }
      visitNonTxKey(ctx, key, command);
      return invokeNextAndFinally(ctx, command, afterWriteCommand);
   }

   /**
    * Visits the {@link WriteCommand} before executing it.
    * <p>
    * The primary owner generates a new {@link IracMetadata} and stores it in the {@link WriteCommand}.
    */
   protected void visitNonTxKey(InvocationContext ctx, Object key, WriteCommand command) {
      LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      int segment = getSegment(command, key);
      if (!cacheTopology.getSegmentDistribution(segment).isPrimary()) {
         return;
      }
      Optional<IracMetadata> entryMetadata = IracUtils.findIracMetadataFromCacheEntry(ctx.lookupEntry(key));
      IracMetadata metadata;
      // RemoveExpired should lose to any other conflicting write
      if (command instanceof RemoveExpiredCommand) {
         metadata = entryMetadata.orElseGet(() -> iracVersionGenerator.generateMetadataWithCurrentVersion(segment));
      } else {
         IracEntryVersion versionSeen = entryMetadata.map(IracMetadata::getVersion).orElse(null);
         metadata = iracVersionGenerator.generateNewMetadata(segment, versionSeen);
      }
      updateCommandMetadata(key, command, metadata);
      if (log.isTraceEnabled()) {
         log.tracef("[IRAC] New metadata for key '%s' is %s. Command=%s", key, metadata, command);
      }
   }

   /**
    * Visits th {@link WriteCommand} after executed and stores the {@link IracMetadata} if it was successful.
    */
   @SuppressWarnings("unused")
   private void handleNonTxDataWriteCommand(InvocationContext ctx, DataWriteCommand command, Object rv, Throwable t) {
      final Object key = command.getKey();
      if (!command.isSuccessful() || skipEntryCommit(ctx, command, key)) {
         return;
      }
      setMetadataToCacheEntry(ctx.lookupEntry(key), command.getSegment(), command.getInternalMetadata(key).iracMetadata());
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
