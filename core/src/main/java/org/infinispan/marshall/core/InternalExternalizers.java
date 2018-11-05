package org.infinispan.marshall.core;

import java.util.Set;

import org.infinispan.atomic.DeltaCompositeKey;
import org.infinispan.atomic.impl.ApplyDelta;
import org.infinispan.atomic.impl.AtomicKeySetImpl;
import org.infinispan.atomic.impl.AtomicMapProxyImpl;
import org.infinispan.cache.impl.EncoderEntryMapper;
import org.infinispan.cache.impl.EncoderKeyMapper;
import org.infinispan.cache.impl.EncoderValueMapper;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commands.functional.functions.MergeFunction;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AdminFlagExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.marshall.exts.EquivalenceExternalizer;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.Immutables;
import org.infinispan.compat.BiFunctionMapper;
import org.infinispan.compat.FunctionMapper;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheValue;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHashFactory;
import org.infinispan.distribution.ch.impl.ScatteredConsistentHash;
import org.infinispan.distribution.ch.impl.ScatteredConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncReplicatedConsistentHashFactory;
import org.infinispan.distribution.ch.impl.TopologyAwareConsistentHashFactory;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyFilter;
import org.infinispan.filter.CompositeKeyValueFilter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.filter.KeyValueFilterAsKeyFilter;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.functional.impl.StatsEnvelope;
import org.infinispan.globalstate.ScopeFilter;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.interceptors.distribution.VersionedResult;
import org.infinispan.interceptors.distribution.VersionedResults;
import org.infinispan.marshall.exts.CacheRpcCommandExternalizer;
import org.infinispan.marshall.exts.CollectionExternalizer;
import org.infinispan.marshall.exts.DoubleSummaryStatisticsExternalizer;
import org.infinispan.marshall.exts.EnumSetExternalizer;
import org.infinispan.marshall.exts.IntSummaryStatisticsExternalizer;
import org.infinispan.marshall.exts.LongSummaryStatisticsExternalizer;
import org.infinispan.marshall.exts.MapExternalizer;
import org.infinispan.marshall.exts.MetaParamExternalizers;
import org.infinispan.marshall.exts.OptionalExternalizer;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.marshall.exts.TriangleAckExternalizer;
import org.infinispan.marshall.exts.UuidExternalizer;
import org.infinispan.marshall.persistence.impl.MarshalledEntryImpl;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventCallable;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerRemoveCallable;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerReplicateCallable;
import org.infinispan.notifications.cachelistener.cluster.MultiClusterEventCallable;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterAsConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterAsKeyValueFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterAsKeyValueFilterConverter;
import org.infinispan.notifications.cachelistener.filter.KeyFilterAsCacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.KeyValueFilterAsCacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.KeyValueFilterConverterAsCacheEventFilterConverter;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.MIMECacheEntry;
import org.infinispan.remoting.responses.BiasRevocationResponse;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTopologyAwareAddress;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.TransactionInfo;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.stream.impl.AbstractCacheStream;
import org.infinispan.stream.impl.EndIterator;
import org.infinispan.stream.impl.IteratorResponses;
import org.infinispan.stream.impl.intops.IntermediateOperationExternalizer;
import org.infinispan.stream.impl.termop.TerminalOperationExternalizer;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ManagerStatusResponse;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.InDoubtTxInfoImpl;
import org.infinispan.transaction.xa.recovery.RecoveryAwareDldGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareGlobalTransaction;
import org.infinispan.transaction.xa.recovery.SerializableXid;
import org.infinispan.util.IntSetExternalizer;
import org.infinispan.util.KeyValuePair;
import org.infinispan.xsite.statetransfer.XSiteState;

final class InternalExternalizers {

   private InternalExternalizers() {
   }

   static ClassToExternalizerMap load(
         GlobalMarshaller marshaller, GlobalComponentRegistry gcr,
         RemoteCommandsFactory cmdFactory) {
      // TODO Add initial value and load factor
      ClassToExternalizerMap exts = new ClassToExternalizerMap(512, 0.6f);

      // Add the stateful externalizer first
      ReplicableCommandExternalizer ext = new ReplicableCommandExternalizer(cmdFactory, gcr);
      addInternalExternalizer(ext, exts);

      // Add the rest of stateless externalizers
      addInternalExternalizer(new AcceptAllKeyValueFilter.Externalizer(), exts);
      addInternalExternalizer(new ApplyDelta.Externalizer(gcr), exts);
      addInternalExternalizer(new AtomicKeySetImpl.Externalizer(gcr), exts);
      addInternalExternalizer(new AtomicKeySetImpl.FunctionExternalizer(), exts);
      addInternalExternalizer(new AtomicMapProxyImpl.Externalizer(), exts);
      addInternalExternalizer(new AvailabilityMode.Externalizer(), exts);
      addInternalExternalizer(new BiasRevocationResponse.Externalizer(), exts);
      addInternalExternalizer(new BiFunctionMapper.Externalizer(), exts);
      addInternalExternalizer(new ByteBufferImpl.Externalizer(), exts);
      addInternalExternalizer(new CacheEventConverterAsConverter.Externalizer(), exts);
      addInternalExternalizer(new CacheEventFilterAsKeyValueFilter.Externalizer(), exts);
      addInternalExternalizer(new CacheEventFilterConverterAsKeyValueFilterConverter.Externalizer(), exts);
      addInternalExternalizer(new CacheFilters.CacheFiltersExternalizer(), exts);
      addInternalExternalizer(new CacheJoinInfo.Externalizer(), exts);
      addInternalExternalizer(new CacheNotFoundResponse.Externalizer(), exts);
      addInternalExternalizer(new CacheRpcCommandExternalizer(gcr, ext), exts);
      addInternalExternalizer(new CacheStatusResponse.Externalizer(), exts);
      addInternalExternalizer(new CacheTopology.Externalizer(), exts);
      addInternalExternalizer(new ClusterEvent.Externalizer(), exts);
      addInternalExternalizer(new ClusterEventCallable.Externalizer(), exts);
      addInternalExternalizer(new ClusterListenerRemoveCallable.Externalizer(), exts);
      addInternalExternalizer(new ClusterListenerReplicateCallable.Externalizer(), exts);
      addInternalExternalizer(new CollectionExternalizer(), exts);
      addInternalExternalizer(new CollectionKeyFilter.Externalizer(), exts);
      addInternalExternalizer(new CompositeKeyFilter.Externalizer(), exts); // TODO: Untested in core
      addInternalExternalizer(new CompositeKeyValueFilter.Externalizer(), exts); // TODO: Untested in core
      addInternalExternalizer(new DefaultConsistentHash.Externalizer(), exts);
      addInternalExternalizer(new DefaultConsistentHashFactory.Externalizer(), exts); // TODO: Untested in core
      addInternalExternalizer(new DeltaCompositeKey.DeltaCompositeKeyExternalizer(), exts);
      addInternalExternalizer(new DldGlobalTransaction.Externalizer(), exts);
      addInternalExternalizer(new DoubleSummaryStatisticsExternalizer(), exts);
      addInternalExternalizer(new EmbeddedMetadata.Externalizer(), exts);
      addInternalExternalizer(new EntryViews.NoValueReadOnlyViewExternalizer(), exts);
      addInternalExternalizer(new EntryViews.ReadWriteSnapshotViewExternalizer(), exts);
      addInternalExternalizer(new EntryViews.ReadOnlySnapshotViewExternalizer(), exts);
      addInternalExternalizer(new EnumSetExternalizer(), exts);
      addInternalExternalizer(new EquivalenceExternalizer(), exts);
      addInternalExternalizer(new ExceptionResponse.Externalizer(), exts);
      addInternalExternalizer(new Flag.Externalizer(), exts);
      addInternalExternalizer(new FunctionMapper.Externalizer(), exts);
      addInternalExternalizer(new GlobalTransaction.Externalizer(), exts);
      addInternalExternalizer(new KeyFilterAsCacheEventFilter.Externalizer(), exts);
      addInternalExternalizer(new KeyFilterAsKeyValueFilter.Externalizer(), exts);
      addInternalExternalizer(new KeyValueFilterConverterAsCacheEventFilterConverter.Externalizer(), exts);
      addInternalExternalizer(new KeyValueFilterAsCacheEventFilter.Externalizer(), exts);
      addInternalExternalizer(new KeyValueFilterAsKeyFilter.Externalizer(), exts); // TODO: Untested in core
      addInternalExternalizer(new ImmortalCacheEntry.Externalizer(), exts);
      addInternalExternalizer(new ImmortalCacheValue.Externalizer(), exts);
      addInternalExternalizer(new Immutables.ImmutableMapWrapperExternalizer(), exts);
      addInternalExternalizer(new Immutables.ImmutableSetWrapperExternalizer(), exts);
      addInternalExternalizer(new InDoubtTxInfoImpl.Externalizer(), exts);
      addInternalExternalizer(new IntermediateOperationExternalizer(), exts);
      addInternalExternalizer(new InternalMetadataImpl.Externalizer(), exts);
      addInternalExternalizer(new IntSummaryStatisticsExternalizer(), exts);
      addInternalExternalizer(new JGroupsAddress.Externalizer(), exts);
      addInternalExternalizer(new JGroupsTopologyAwareAddress.Externalizer(), exts);
      addInternalExternalizer(new LongSummaryStatisticsExternalizer(), exts);
      addInternalExternalizer(new KeyValuePair.Externalizer(), exts);
      addInternalExternalizer(new ManagerStatusResponse.Externalizer(), exts);
      addInternalExternalizer(new MapExternalizer(), exts);
      addInternalExternalizer(new MarshallableFunctionExternalizers.ConstantLambdaExternalizer(), exts);
      addInternalExternalizer(new MarshallableFunctionExternalizers.LambdaWithMetasExternalizer(), exts);
      addInternalExternalizer(new MarshallableFunctionExternalizers.SetValueIfEqualsReturnBooleanExternalizer(), exts);
      addInternalExternalizer(new MarshalledEntryImpl.Externalizer(marshaller), exts);
      addInternalExternalizer(new MergeFunction.Externalizer(), exts);
      addInternalExternalizer(new MetadataImmortalCacheEntry.Externalizer(), exts);
      addInternalExternalizer(new MetadataImmortalCacheValue.Externalizer(), exts);
      addInternalExternalizer(new MetadataMortalCacheEntry.Externalizer(), exts);
      addInternalExternalizer(new MetadataMortalCacheValue.Externalizer(), exts);
      addInternalExternalizer(new MetadataTransientCacheEntry.Externalizer(), exts); // TODO: Untested in core
      addInternalExternalizer(new MetadataTransientCacheValue.Externalizer(), exts); // TODO: Untested in core
      addInternalExternalizer(new MetadataTransientMortalCacheEntry.Externalizer(), exts);
      addInternalExternalizer(new MetadataTransientMortalCacheValue.Externalizer(), exts); // TODO: Untested in core
      addInternalExternalizer(new MetaParamExternalizers.LifespanExternalizer(), exts);
      addInternalExternalizer(new MetaParamExternalizers.EntryVersionParamExternalizer(), exts);
      addInternalExternalizer(new MetaParamExternalizers.MaxIdleExternalizer(), exts);
      addInternalExternalizer(new MetaParamsInternalMetadata.Externalizer(), exts);
      addInternalExternalizer(new MIMECacheEntry.Externalizer(), exts); // new
      addInternalExternalizer(new MortalCacheEntry.Externalizer(), exts);
      addInternalExternalizer(new MortalCacheValue.Externalizer(), exts);
      addInternalExternalizer(new MultiClusterEventCallable.Externalizer(), exts);
      addInternalExternalizer(new MurmurHash3.Externalizer(), exts);
      addInternalExternalizer(new NumericVersion.Externalizer(), exts);
      addInternalExternalizer(new OptionalExternalizer(), exts);
      addInternalExternalizer(new PersistentUUID.Externalizer(), exts);
      addInternalExternalizer(new RecoveryAwareDldGlobalTransaction.Externalizer(), exts);
      addInternalExternalizer(new RecoveryAwareGlobalTransaction.Externalizer(), exts);
      addInternalExternalizer(new RemoteMetadata.Externalizer(), exts);
      addInternalExternalizer(new ReplicatedConsistentHash.Externalizer(), exts);
      addInternalExternalizer(new ReplicatedConsistentHashFactory.Externalizer(), exts); // TODO: Untested in core
      addInternalExternalizer(new ScatteredConsistentHash.Externalizer(), exts);
      addInternalExternalizer(new ScatteredConsistentHashFactory.Externalizer(), exts);
      addInternalExternalizer(new SerializableXid.XidExternalizer(), exts);
      addInternalExternalizer(new SimpleClusteredVersion.Externalizer(), exts);
      addInternalExternalizer(new StateChunk.Externalizer(), exts);
      addInternalExternalizer(new StatsEnvelope.Externalizer(), exts);
      addInternalExternalizer(new StreamMarshalling.StreamMarshallingExternalizer(), exts);
      addInternalExternalizer(new SuccessfulResponse.Externalizer(), exts);
      addInternalExternalizer(new SyncConsistentHashFactory.Externalizer(), exts);
      addInternalExternalizer(new SyncReplicatedConsistentHashFactory.Externalizer(), exts);
      addInternalExternalizer(new TerminalOperationExternalizer(), exts);
      addInternalExternalizer(new TopologyAwareConsistentHashFactory.Externalizer(), exts); // TODO: Untested in core
      addInternalExternalizer(new TopologyAwareSyncConsistentHashFactory.Externalizer(), exts);
      addInternalExternalizer(new TransactionInfo.Externalizer(), exts);
      addInternalExternalizer(new TransientCacheEntry.Externalizer(), exts);
      addInternalExternalizer(new TransientCacheValue.Externalizer(), exts);
      addInternalExternalizer(new TransientMortalCacheEntry.Externalizer(), exts);
      addInternalExternalizer(new TransientMortalCacheValue.Externalizer(), exts);
      addInternalExternalizer(new UnsuccessfulResponse.Externalizer(), exts);
      addInternalExternalizer(new UnsureResponse.Externalizer(), exts);
      addInternalExternalizer(new UuidExternalizer(), exts);
      addInternalExternalizer(new VersionedResult.Externalizer(), exts);
      addInternalExternalizer(new VersionedResults.Externalizer(), exts);
      addInternalExternalizer(new WrappedByteArray.Externalizer(), exts);
      addInternalExternalizer(new XSiteState.XSiteStateExternalizer(), exts);
      addInternalExternalizer(new TriangleAckExternalizer(), exts);
      addInternalExternalizer(new IteratorResponses.IteratorResponsesExternalizer(), exts);
      addInternalExternalizer(new EndIterator.EndIteratorExternalizer(), exts);
      addInternalExternalizer(XidImpl.EXTERNALIZER, exts);
      addInternalExternalizer(new EncoderKeyMapper.Externalizer(), exts);
      addInternalExternalizer(new EncoderValueMapper.Externalizer(), exts);
      addInternalExternalizer(new EncoderEntryMapper.Externalizer(), exts);
      addInternalExternalizer(new IntSetExternalizer(), exts);
      addInternalExternalizer(new AbstractCacheStream.MapOpsExternalizer(), exts);
      addInternalExternalizer(new DataConversion.Externalizer(), exts);
      addInternalExternalizer(new ScopedState.Externalizer(), exts);
      addInternalExternalizer(new ScopeFilter.Externalizer(), exts);
      addInternalExternalizer(new AdminFlagExternalizer(), exts);

      return exts;
   }

   private static void addInternalExternalizer(
         AdvancedExternalizer ext, ClassToExternalizerMap exts) {
      Set<Class<?>> subTypes = ext.getTypeClasses();
      for (Class<?> subType : subTypes)
         exts.put(subType, ext);
   }

}
