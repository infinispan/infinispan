package org.infinispan.marshall.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import org.infinispan.atomic.DeltaCompositeKey;
import org.infinispan.atomic.impl.AtomicHashMap;
import org.infinispan.atomic.impl.AtomicHashMapDelta;
import org.infinispan.atomic.impl.ClearOperation;
import org.infinispan.atomic.impl.PutOperation;
import org.infinispan.atomic.impl.RemoveOperation;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.LambdaExternalizer;
import org.infinispan.commons.marshall.MarshallableFunctionExternalizers;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.ImmutableListCopy;
import org.infinispan.commons.util.Immutables;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
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
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHash;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncReplicatedConsistentHashFactory;
import org.infinispan.distribution.ch.impl.TopologyAwareConsistentHashFactory;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.AcceptAllKeyValueFilter;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.CompositeKeyFilter;
import org.infinispan.filter.CompositeKeyValueFilter;
import org.infinispan.filter.KeyFilterAsKeyValueFilter;
import org.infinispan.filter.KeyValueFilterAsKeyFilter;
import org.infinispan.filter.NullValueConverter;
import org.infinispan.functional.impl.EntryViews;
import org.infinispan.functional.impl.MetaParams;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.marshall.exts.ArrayExternalizers;
import org.infinispan.marshall.exts.CacheRpcCommandExternalizer;
import org.infinispan.marshall.exts.DoubleSummaryStatisticsExternalizer;
import org.infinispan.marshall.exts.EnumSetExternalizer;
import org.infinispan.marshall.exts.IntSummaryStatisticsExternalizer;
import org.infinispan.marshall.exts.ListExternalizer;
import org.infinispan.marshall.exts.LongSummaryStatisticsExternalizer;
import org.infinispan.marshall.exts.MapExternalizer;
import org.infinispan.marshall.exts.MetaParamExternalizers;
import org.infinispan.marshall.exts.OptionalExternalizer;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.marshall.exts.SetExternalizer;
import org.infinispan.marshall.exts.SingletonListExternalizer;
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
import org.infinispan.notifications.cachelistener.filter.ConverterAsCacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.KeyFilterAsCacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.KeyValueFilterAsCacheEventFilter;
import org.infinispan.partitionhandling.AvailabilityMode;
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
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

/**
 * The externalizer table maintains information necessary to be able to map a particular type with the corresponding
 * {@link org.infinispan.commons.marshall.AdvancedExternalizer} implementation that it marshall, and it also keeps information of which {@link org.infinispan.commons.marshall.AdvancedExternalizer}
 * should be used to read data from a buffer given a particular {@link org.infinispan.commons.marshall.AdvancedExternalizer} identifier.
 *
 * These tables govern how either internal Infinispan classes, or user defined classes, are marshalled to a given
 * output, or how these are unmarshalled from a given input.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Scope(Scopes.GLOBAL)
public class ExternalizerTable implements ObjectTable {
   private static final Log log = LogFactory.getLog(ExternalizerTable.class);
   private static final boolean trace = log.isTraceEnabled();

   /**
    * Contains mapping of classes to their corresponding externalizer classes via ExternalizerAdapter instances.
    */
   private final Map<Class<?>, ExternalizerAdapter> writers = new WeakHashMap<Class<?>, ExternalizerAdapter>();

   /**
    * Contains mapping of ids to their corresponding AdvancedExternalizer classes via ExternalizerAdapter instances.
    * This maps contains mappings for both internal and foreign or user defined externalizers.
    *
    * Internal ids are only allowed to be unsigned bytes (0 to 254). 255 is an special id that signals the
    * arrival of a foreign externalizer id. Foreign externalizers are only allowed to use positive ids that between 0
    * and Integer.MAX_INT. To avoid clashes between foreign and internal ids, foreign ids are transformed into negative
    * values to be stored in this map. This way, we avoid the need of a second map to hold user defined externalizers.
    */
   private final Map<Integer, ExternalizerAdapter> readers = new HashMap<Integer, ExternalizerAdapter>();

   private volatile boolean started;

   private RemoteCommandsFactory cmdFactory;
   private GlobalComponentRegistry gcr;
   private StreamingMarshaller globalMarshaller;

   @Inject
   public void inject(RemoteCommandsFactory cmdFactory, GlobalComponentRegistry gcr,
         StreamingMarshaller globalMarshaller) {
      this.cmdFactory = cmdFactory;
      this.gcr = gcr;
      this.globalMarshaller = globalMarshaller;
   }

   @Start(priority = 7) // Should start before global marshaller
   public void start() {
      loadInternalMarshallables();
      loadForeignMarshallables(gcr.getGlobalConfiguration());
      started = true;
      if (trace) {
         log.tracef("Constant object table was started and contains these externalizer readers: %s", readers);
         log.tracef("The externalizer writers collection contains: %s", writers);
      }
   }

   @Stop(priority = 13) // Stop after global marshaller
   public void stop() {
      started = false;
      writers.clear();
      readers.clear();
      log.trace("Externalizer reader and writer maps have been cleared and constant object table was stopped");
   }

   @Override
   public Writer getObjectWriter(Object o) throws IOException {
      Class<?> clazz = o.getClass();
      if (!started) {
         throw log.externalizerTableStopped(clazz.getName());
      }
      Writer writer = writers.get(clazz);
      return writer;
   }

   @Override
   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      int readerIndex = input.readUnsignedByte();
      int foreignId = -1;
      if (readerIndex == Ids.MAX_ID) {
         // User defined externalizer
         foreignId = UnsignedNumeric.readUnsignedInt(input);
         readerIndex = generateForeignReaderIndex(foreignId);
      }

      ExternalizerAdapter adapter = readers.get(readerIndex);
      if (adapter == null) {
         if (!started) {
            log.tracef("Either the marshaller has stopped or hasn't started. Read externalizers are not properly populated: %s", readers);

            if (Thread.currentThread().isInterrupted()) {
               throw log.pushReadInterruptionDueToCacheManagerShutdown(readerIndex, new InterruptedException());
            } else {
               throw log.cannotResolveExternalizerReader(gcr.getStatus(), readerIndex);
            }
         } else {
            if (trace) {
               log.tracef("Unknown type. Input stream has %s to read", input.available());
               log.tracef("Check contents of read externalizers: %s", readers);
            }

            if (foreignId > 0)
               throw log.missingForeignExternalizer(foreignId);

            throw log.unknownExternalizerReaderIndex(readerIndex);
         }
      }

      return adapter.readObject(input);
   }

   public Externalizer getExternalizer(Object o) {
      ExternalizerAdapter adapter = writers.get(o.getClass());
      if (adapter != null && adapter.externalizer instanceof LambdaExternalizer)
         return adapter.externalizer;

      return null;
   }

   boolean isMarshallableCandidate(Object o) {
      return writers.containsKey(o.getClass());
   }

   int getExternalizerId(Object o) {
      return writers.get(o.getClass()).getExternalizerId();
   }

   private void loadInternalMarshallables() {
      addInternalExternalizer(new ListExternalizer());
      addInternalExternalizer(new MapExternalizer());
      addInternalExternalizer(new SetExternalizer());
      addInternalExternalizer(new EnumSetExternalizer());
      addInternalExternalizer(new ArrayExternalizer.ListArray());
      addInternalExternalizer(new SingletonListExternalizer());

      addInternalExternalizer(new IntSummaryStatisticsExternalizer());
      addInternalExternalizer(new LongSummaryStatisticsExternalizer());
      addInternalExternalizer(new DoubleSummaryStatisticsExternalizer());

      addInternalExternalizer(new GlobalTransaction.Externalizer());
      addInternalExternalizer(new RecoveryAwareGlobalTransaction.Externalizer());
      addInternalExternalizer(new DldGlobalTransaction.Externalizer());
      addInternalExternalizer(new RecoveryAwareDldGlobalTransaction.Externalizer());
      addInternalExternalizer(new JGroupsAddress.Externalizer());
      addInternalExternalizer(new ImmutableListCopy.Externalizer());
      addInternalExternalizer(new Immutables.ImmutableMapWrapperExternalizer());
      addInternalExternalizer(new MarshalledValue.Externalizer(globalMarshaller));
      addInternalExternalizer(new ByteBufferImpl.Externalizer());

      addInternalExternalizer(new SuccessfulResponse.Externalizer());
      addInternalExternalizer(new ExceptionResponse.Externalizer());
      addInternalExternalizer(new UnsuccessfulResponse.Externalizer());
      addInternalExternalizer(new UnsureResponse.Externalizer());
      addInternalExternalizer(new CacheNotFoundResponse.Externalizer());

      ReplicableCommandExternalizer cmExt =
            new ReplicableCommandExternalizer(cmdFactory, gcr);
      addInternalExternalizer(cmExt);
      addInternalExternalizer(new CacheRpcCommandExternalizer(gcr, cmExt));

      addInternalExternalizer(new ImmortalCacheEntry.Externalizer());
      addInternalExternalizer(new MortalCacheEntry.Externalizer());
      addInternalExternalizer(new TransientCacheEntry.Externalizer());
      addInternalExternalizer(new TransientMortalCacheEntry.Externalizer());
      addInternalExternalizer(new ImmortalCacheValue.Externalizer());
      addInternalExternalizer(new MortalCacheValue.Externalizer());
      addInternalExternalizer(new TransientCacheValue.Externalizer());
      addInternalExternalizer(new TransientMortalCacheValue.Externalizer());

      addInternalExternalizer(new SimpleClusteredVersion.Externalizer());
      addInternalExternalizer(new MetadataImmortalCacheEntry.Externalizer());
      addInternalExternalizer(new MetadataMortalCacheEntry.Externalizer());
      addInternalExternalizer(new MetadataTransientCacheEntry.Externalizer());
      addInternalExternalizer(new MetadataTransientMortalCacheEntry.Externalizer());
      addInternalExternalizer(new MetadataImmortalCacheValue.Externalizer());
      addInternalExternalizer(new MetadataMortalCacheValue.Externalizer());
      addInternalExternalizer(new MetadataTransientCacheValue.Externalizer());
      addInternalExternalizer(new MetadataTransientMortalCacheValue.Externalizer());

      addInternalExternalizer(new DeltaCompositeKey.DeltaCompositeKeyExternalizer());
      addInternalExternalizer(new AtomicHashMap.Externalizer());
      addInternalExternalizer(new AtomicHashMapDelta.Externalizer());
      addInternalExternalizer(new PutOperation.Externalizer());
      addInternalExternalizer(new RemoveOperation.Externalizer());
      addInternalExternalizer(new ClearOperation.Externalizer());
      addInternalExternalizer(new JGroupsTopologyAwareAddress.Externalizer());

      addInternalExternalizer(new SerializableXid.XidExternalizer());
      addInternalExternalizer(new InDoubtTxInfoImpl.Externalizer());

      addInternalExternalizer(new MurmurHash3.Externalizer());
      addInternalExternalizer(new HashFunctionPartitioner.Externalizer());
      addInternalExternalizer(new AffinityPartitioner.Externalizer());

      addInternalExternalizer(new DefaultConsistentHash.Externalizer());
      addInternalExternalizer(new ReplicatedConsistentHash.Externalizer());
      addInternalExternalizer(new DefaultConsistentHashFactory.Externalizer());
      addInternalExternalizer(new ReplicatedConsistentHashFactory.Externalizer());
      addInternalExternalizer(new SyncConsistentHashFactory.Externalizer());
      addInternalExternalizer(new SyncReplicatedConsistentHashFactory.Externalizer());
      addInternalExternalizer(new TopologyAwareConsistentHashFactory.Externalizer());
      addInternalExternalizer(new TopologyAwareSyncConsistentHashFactory.Externalizer());
      addInternalExternalizer(new CacheTopology.Externalizer());
      addInternalExternalizer(new CacheJoinInfo.Externalizer());
      addInternalExternalizer(new TransactionInfo.Externalizer());
      addInternalExternalizer(new StateChunk.Externalizer());

      addInternalExternalizer(new Flag.Externalizer());
      addInternalExternalizer(new ValueMatcher.Externalizer());
      addInternalExternalizer(new AvailabilityMode.Externalizer());

      addInternalExternalizer(new EmbeddedMetadata.Externalizer());

      addInternalExternalizer(new NumericVersion.Externalizer());
      addInternalExternalizer(new KeyValuePair.Externalizer());
      addInternalExternalizer(new InternalMetadataImpl.Externalizer());
      addInternalExternalizer(new MarshalledEntryImpl.Externalizer(globalMarshaller));

      addInternalExternalizer(new CollectionKeyFilter.Externalizer());
      addInternalExternalizer(new KeyFilterAsKeyValueFilter.Externalizer());
      addInternalExternalizer(new KeyValueFilterAsKeyFilter.Externalizer());
      addInternalExternalizer(new ClusterEvent.Externalizer());
      addInternalExternalizer(new ClusterEventCallable.Externalizer());
      addInternalExternalizer(new ClusterListenerRemoveCallable.Externalizer());
      addInternalExternalizer(new ClusterListenerReplicateCallable.Externalizer());
      addInternalExternalizer(new XSiteState.XSiteStateExternalizer());
      addInternalExternalizer(new CompositeKeyFilter.Externalizer());
      addInternalExternalizer(new CompositeKeyValueFilter.Externalizer());
      addInternalExternalizer(new CacheStatusResponse.Externalizer());
      addInternalExternalizer(new CacheEventConverterAsConverter.Externalizer());
      addInternalExternalizer(new CacheEventFilterAsKeyValueFilter.Externalizer());
      addInternalExternalizer(new CacheEventFilterConverterAsKeyValueFilterConverter.Externalizer());
      addInternalExternalizer(new ConverterAsCacheEventConverter.Externalizer());
      addInternalExternalizer(new KeyFilterAsCacheEventFilter.Externalizer());
      addInternalExternalizer(new KeyValueFilterAsCacheEventFilter.Externalizer());
      addInternalExternalizer(new NullValueConverter.Externalizer());
      addInternalExternalizer(new AcceptAllKeyValueFilter.Externalizer());
      addInternalExternalizer(new ManagerStatusResponse.Externalizer());
      addInternalExternalizer(new MultiClusterEventCallable.Externalizer());

      addInternalExternalizer(new IntermediateOperationExternalizer());
      addInternalExternalizer(new TerminalOperationExternalizer());
      addInternalExternalizer(new StreamMarshalling.StreamMarshallingExternalizer());
      addInternalExternalizer(new CommandInvocationId.Externalizer());
      addInternalExternalizer(new CacheFilters.CacheFiltersExternalizer());


      addInternalExternalizer(new OptionalExternalizer());

      addInternalExternalizer(new MetaParamsInternalMetadata.Externalizer());
      addInternalExternalizer(new MetaParams.Externalizer());

      // TODO: Add other MetaParam externalizers
      addInternalExternalizer(new MetaParamExternalizers.LifespanExternalizer());
      addInternalExternalizer(new MetaParamExternalizers.EntryVersionParamExternalizer());
      addInternalExternalizer(new MetaParamExternalizers.NumericEntryVersionExternalizer());

      addInternalExternalizer(new EntryViews.ReadWriteSnapshotViewExternalizer());
      addInternalExternalizer(new MarshallableFunctionExternalizers.ConstantLambdaExternalizer());
      addInternalExternalizer(new MarshallableFunctionExternalizers.LambdaWithMetasExternalizer());
      addInternalExternalizer(new MarshallableFunctionExternalizers.SetValueIfEqualsReturnBooleanExternalizer());
      addInternalExternalizer(new PersistentUUID.Externalizer());

      addInternalExternalizer(new Immutables.ImmutableEntryExternalizer());
   }

   void addInternalExternalizer(AdvancedExternalizer<?> ext) {
      int id = checkInternalIdLimit(ext.getId(), ext);
      updateExtReadersWritersWithTypes(new ExternalizerAdapter(id, ext));
   }

   private void updateExtReadersWritersWithTypes(ExternalizerAdapter adapter) {
      updateExtReadersWritersWithTypes(adapter, adapter.id);
   }

   private void updateExtReadersWritersWithTypes(ExternalizerAdapter adapter, int readerIndex) {
      Set<Class<?>> typeClasses = adapter.externalizer.getTypeClasses();
      if (typeClasses.size() > 0) {
         for (Class<?> typeClass : typeClasses)
            updateExtReadersWriters(adapter, typeClass, readerIndex);
      } else {
         throw log.advanceExternalizerTypeClassesUndefined(adapter.externalizer.getClass().getName());
      }
   }

   private void loadForeignMarshallables(GlobalConfiguration globalCfg) {
      log.trace("Loading user defined externalizers");
      for (Map.Entry<Integer, AdvancedExternalizer<?>> config : globalCfg.serialization().advancedExternalizers().entrySet()) {
         AdvancedExternalizer<?> ext = config.getValue();

         // If no XML or programmatic config, id in annotation is used
         // as long as it's not default one (meaning, user did not set it).
         // If XML or programmatic config in use ignore @Marshalls annotation and use value in config.
         Integer id = ext.getId();
         if (config.getKey() == null && id == null)
            throw new CacheConfigurationException(String.format(
                  "No advanced externalizer identifier set for externalizer %s",
                  ext.getClass().getName()));
         else if (config.getKey() != null)
            id = config.getKey();

         id = checkForeignIdLimit(id, ext);
         updateExtReadersWritersWithTypes(new ForeignExternalizerAdapter(id, ext), generateForeignReaderIndex(id));
      }
   }

   private void updateExtReadersWriters(ExternalizerAdapter adapter, Class<?> typeClass, int readerIndex) {
      writers.put(typeClass, adapter);
      ExternalizerAdapter prevReader = readers.put(readerIndex, adapter);
      // Several externalizers might share same id (i.e. HashMap and TreeMap use MapExternalizer)
      // but a duplicate is only considered when that particular index has already been entered
      // in the readers map and the externalizers are different (they're from different classes)
      if (prevReader != null && !prevReader.equals(adapter))
         throw log.duplicateExternalizerIdFound(
               adapter.id, typeClass, prevReader.externalizer.getClass().getName(), readerIndex);

      if (trace)
         log.tracef("Loaded externalizer %s for %s with id %s and reader index %s",
                   adapter.externalizer.getClass().getName(), typeClass, adapter.id, readerIndex);

   }

   private int checkInternalIdLimit(int id, AdvancedExternalizer<?> ext) {
      if (id >= Ids.MAX_ID)
         throw log.internalExternalizerIdLimitExceeded(ext, id, Ids.MAX_ID);

      return id;
   }

   private int checkForeignIdLimit(int id, AdvancedExternalizer<?> ext) {
      if (id < 0)
         throw log.foreignExternalizerUsingNegativeId(ext, id);

      return id;
   }

   private int generateForeignReaderIndex(int foreignId) {
      return 0x80000000 | foreignId;
   }

   static class ExternalizerAdapter implements Writer {
      final int id;
      final AdvancedExternalizer<Object> externalizer;

      ExternalizerAdapter(int id, AdvancedExternalizer<?> externalizer) {
         this.id = id;
         this.externalizer = (AdvancedExternalizer<Object>) externalizer;
      }

      public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
         return externalizer.readObject(input);
      }

      @Override
      public void writeObject(Marshaller output, Object object) throws IOException {
         output.write(id);
         externalizer.writeObject(output, object);
      }

      int getExternalizerId() {
         return id;
      }

      @Override
      public String toString() {
         // Each adapter is represented by the externalizer it delegates to, so just return the class name
         return externalizer.getClass().getName();
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ExternalizerAdapter that = (ExternalizerAdapter) o;
         if (id != that.id) return false;
         if (externalizer != null ? !externalizer.getClass().equals(that.externalizer.getClass()) : that.externalizer != null) return false;
         return true;
      }

      @Override
      public int hashCode() {
         int result = id;
         result = 31 * result + (externalizer.getClass() != null ? externalizer.getClass().hashCode() : 0);
         return result;
      }
   }

   static class ForeignExternalizerAdapter extends ExternalizerAdapter {
      final int foreignId;

      ForeignExternalizerAdapter(int foreignId, AdvancedExternalizer<?> externalizer) {
         super(Ids.MAX_ID, externalizer);
         this.foreignId = foreignId;
      }

      @Override
      int getExternalizerId() {
         return foreignId;
      }

      @Override
      public void writeObject(Marshaller output, Object object) throws IOException {
         output.write(id);
         // Write as an unsigned, variable length, integer to safe space
         UnsignedNumeric.writeUnsignedInt(output, foreignId);
         externalizer.writeObject(output, object);
      }
   }
}
