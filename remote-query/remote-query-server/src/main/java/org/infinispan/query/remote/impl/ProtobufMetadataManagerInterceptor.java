package org.infinispan.query.remote.impl;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTO_KEY_SUFFIX;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.SyncInvocationStage;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.FileDescriptor;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.remote.impl.logging.Log;
import org.infinispan.util.KeyValuePair;

/**
 * Intercepts updates to the protobuf schema file caches and updates the SerializationContext accordingly.
 *
 * <p>Must be in the interceptor chain after {@code EntryWrappingInterceptor} so that it can fail a write after
 * {@code CallInterceptor} updates the context entry but before {@code EntryWrappingInterceptor} commits the entry.</p>
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class ProtobufMetadataManagerInterceptor extends BaseCustomAsyncInterceptor {

   private static final Log log = LogFactory.getLog(ProtobufMetadataManagerInterceptor.class, Log.class);

   private static final Metadata DEFAULT_METADATA = new EmbeddedMetadata.Builder().build();

   private CommandsFactory commandsFactory;

   private ComponentRef<AsyncInterceptorChain> invoker;

   private SerializationContext serializationContext;

   private KeyPartitioner keyPartitioner;

   private SerializationContextRegistry serializationContextRegistry;

   /**
    * A no-op callback.
    */
   private static final FileDescriptorSource.ProgressCallback EMPTY_CALLBACK = new FileDescriptorSource.ProgressCallback() {
   };

   private final static class ProgressCallback implements FileDescriptorSource.ProgressCallback {
      private final Map<String, DescriptorParserException> errorFiles = new TreeMap<>();
      private final Set<String> successFiles = new TreeSet<>();

      Map<String, DescriptorParserException> getErrorFiles() {
         return errorFiles;
      }

      public Set<String> getSuccessFiles() {
         return successFiles;
      }

      @Override
      public void handleError(String fileName, DescriptorParserException exception) {
         // handle first error per file, ignore the rest if any
         errorFiles.putIfAbsent(fileName, exception);
      }

      @Override
      public void handleSuccess(String fileName) {
         // a file that is already failed cannot be added as success
         if (!errorFiles.containsKey(fileName)) {
            successFiles.add(fileName);
         }
      }
   }

   /**
    * Visitor used for handling the list of modifications of a PrepareCommand.
    */
   private final AbstractVisitor serializationContextUpdaterVisitor = new AbstractVisitor() {

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         final String key = (String) command.getKey();
         if (shouldIntercept(key)) {
            registerProtoFile(key, (String) command.getValue(), EMPTY_CALLBACK);
         }
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
         final Map<Object, Object> map = command.getMap();
         FileDescriptorSource source = new FileDescriptorSource().withProgressCallback(EMPTY_CALLBACK);
         for (Object key : map.keySet()) {
            if (shouldIntercept(key)) {
               source.addProtoFile((String) key, (String) map.get(key));
            }
         }
         registerFileDescriptorSource(source, source.getFiles().keySet().toString());
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
         final String key = (String) command.getKey();
         if (shouldIntercept(key)) {
            registerProtoFile(key, (String) command.getNewValue(), EMPTY_CALLBACK);
         }
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
         final String key = (String) command.getKey();
         if (shouldIntercept(key)) {
            if (serializationContext.getFileDescriptors().containsKey(key)) {
               serializationContext.unregisterProtoFile(key);
            }
         }
         return null;
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
         for (String fileName : serializationContext.getFileDescriptors().keySet()) {
            serializationContext.unregisterProtoFile(fileName);
         }
         return null;
      }
   };

   private void registerProtoFile(String name, String content, FileDescriptorSource.ProgressCallback callback) {
      FileDescriptorSource source = new FileDescriptorSource()
            .withProgressCallback(callback)
            .addProtoFile(name, content);
      registerFileDescriptorSource(source, source.getFiles().keySet().toString());
   }

   @Inject
   public void init(CommandsFactory commandsFactory, ComponentRef<AsyncInterceptorChain> invoker, KeyPartitioner keyPartitioner,
                    ProtobufMetadataManager protobufMetadataManager, SerializationContextRegistry serializationContextRegistry) {
      this.commandsFactory = commandsFactory;
      this.invoker = invoker;
      this.keyPartitioner = keyPartitioner;
      this.serializationContext = ((ProtobufMetadataManagerImpl) protobufMetadataManager).getSerializationContext();
      this.serializationContextRegistry = serializationContextRegistry;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         if (!rCtx.isOriginLocal()) {
            // apply updates to the serialization context
            for (WriteCommand wc : rCommand.getModifications()) {
               wc.acceptVisitor(rCtx, serializationContextUpdaterVisitor);
            }
         }
      });
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      final Object key = command.getKey();
      if (!(key instanceof String)) {
         throw log.keyMustBeString(key.getClass());
      }

      if (!shouldIntercept(key)) {
         return invokeNext(ctx, command);
      }

      InvocationStage stage;
      if (ctx.isOriginLocal() && !command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER | FlagBitSets.SKIP_LOCKING)) {
         if (!((String) key).endsWith(PROTO_KEY_SUFFIX)) {
            throw log.keyMustBeStringEndingWithProto(key);
         }

         // lock global errors key
         LockControlCommand cmd = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, command.getFlagsBitSet(), null);
         stage = invoker.running().invokeStage(ctx, cmd);
      } else {
         stage = SyncInvocationStage.completedNullStage();
      }

      return makeStage(asyncInvokeNext(ctx, command, stage))
            .thenApply(ctx, command, this::handlePutKeyValueResult);
   }

   private InvocationStage handlePutKeyValueResult(InvocationContext ctx, PutKeyValueCommand putKeyValueCommand, Object rv) {
      if (putKeyValueCommand.isSuccessful()) {
         // StateConsumerImpl uses PutKeyValueCommands with InternalCacheEntry
         // values in order to preserve timestamps, so read the value from the context
         Object key = putKeyValueCommand.getKey();
         Object value = ctx.lookupEntry(key).getValue();
         if (!(value instanceof String)) {
            throw log.valueMustBeString(value.getClass());
         }

         long flagsBitSet = copyFlags(putKeyValueCommand);
         if (ctx.isOriginLocal() && !putKeyValueCommand.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            ProgressCallback progressCallback = new ProgressCallback();
            registerProtoFile((String) key, (String) value, progressCallback);

            List<KeyValuePair<String, String>> errorUpdates = computeErrorUpdates(progressCallback);
            InvocationStage updateStage = updateSchemaErrorsIterator(ctx, flagsBitSet, errorUpdates.iterator());
            return makeStage(updateStage.thenReturn(ctx, putKeyValueCommand, rv));
         }
         registerProtoFile((String) key, (String) value, EMPTY_CALLBACK);
      }
      return makeStage(rv);
   }

   List<KeyValuePair<String, String>> computeErrorUpdates(ProgressCallback progressCallback) {
      // List of updated proto schemas and their errors
      // Proto schemas with no errors have a null value
      List<KeyValuePair<String, String>> errorUpdates = new ArrayList<>();

      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, DescriptorParserException> errorEntry : progressCallback.getErrorFiles().entrySet()) {
         String fdName = errorEntry.getKey();
         String errorValue = errorEntry.getValue().getMessage();
         if (sb.length() > 0) {
            sb.append('\n');
         }
         sb.append(fdName);
         errorUpdates.add(KeyValuePair.of(fdName, errorValue));
      }

      for (String successKeyName : progressCallback.getSuccessFiles()) {
         errorUpdates.add(KeyValuePair.of(successKeyName, null));
      }
      errorUpdates.add(KeyValuePair.of("", sb.length() > 0 ? sb.toString() : null));
      return errorUpdates;
   }

   /**
    * For preload, we need to copy the CACHE_MODE_LOCAL flag from the put command.
    * But we also need to remove the SKIP_CACHE_STORE flag, so that existing .errors keys are updated.
    */
   private long copyFlags(FlagAffectedCommand command) {
      return command.getFlagsBitSet() | FlagBitSets.IGNORE_RETURN_VALUES & ~FlagBitSets.SKIP_CACHE_STORE;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }

      final Map<Object, Object> map = command.getMap();

      FileDescriptorSource source = new FileDescriptorSource();
      for (Object key : map.keySet()) {
         final Object value = map.get(key);
         if (!(key instanceof String)) {
            throw log.keyMustBeString(key.getClass());
         }
         if (!(value instanceof String)) {
            throw log.valueMustBeString(value.getClass());
         }
         if (shouldIntercept(key)) {
            if (!((String) key).endsWith(PROTO_KEY_SUFFIX)) {
               throw log.keyMustBeStringEndingWithProto(key);
            }
            source.addProtoFile((String) key, (String) value);
         }
      }

      // lock global errors key
      VisitableCommand cmd = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, command.getFlagsBitSet(), null);
      InvocationStage stage = invoker.running().invokeStage(ctx, cmd);

      return makeStage(asyncInvokeNext(ctx, command, stage)).thenApply(ctx, command, (rCtx, rCommand, rv) -> {
         long flagsBitSet = copyFlags(rCommand);
         ProgressCallback progressCallback = null;
         if (rCtx.isOriginLocal()) {
            progressCallback = new ProgressCallback();
            source.withProgressCallback(progressCallback);
         } else {
            source.withProgressCallback(EMPTY_CALLBACK);
         }
         registerFileDescriptorSource(source, source.getFiles().keySet().toString());

         if (progressCallback != null) {
            List<KeyValuePair<String, String>> errorUpdates = computeErrorUpdates(progressCallback);
            return updateSchemaErrorsIterator(rCtx, flagsBitSet, errorUpdates.iterator());
         }
         return InvocationStage.completedNullStage();
      });
   }

   private void registerFileDescriptorSource(FileDescriptorSource source, String fileNameString) {
      try {
         // Register protoFiles with remote-query context
         serializationContext.registerProtoFiles(source);

         // Register schema with user context to allow transcoding
         serializationContextRegistry.addProtoFile(SerializationContextRegistry.MarshallerType.USER, source);
      } catch (DescriptorParserException e) {
         throw log.failedToParseProtoFile(fileNameString, e);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      if (!(command.getKey() instanceof String)) {
         throw log.keyMustBeString(command.getKey().getClass());
      }
      String key = (String) command.getKey();
      if (!shouldIntercept(key)) {
         return invokeNext(ctx, command);
      }
      // lock global errors key
      long flagsBitSet = copyFlags(command);
      LockControlCommand lockCommand = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, flagsBitSet, null);

      InvocationStage stage = invoker.running().invokeStage(ctx, lockCommand);
      stage = stage.thenApplyMakeStage(ctx, command, (rCtx, rCommand, __) -> {
         if (serializationContext.getFileDescriptors().containsKey(key)) {
            serializationContext.unregisterProtoFile(key);
         }
         if (serializationContextRegistry.getUserCtx().getFileDescriptors().containsKey(key)) {
            serializationContextRegistry.removeProtoFile(SerializationContextRegistry.MarshallerType.USER, key);
         }

         // put error key for all unresolved files and remove error key for all resolved files
         // Error keys to be removed have a null value
         List<KeyValuePair<String, String>> errorUpdates = computeErrorUpdatesAfterRemove(key);
         return updateSchemaErrorsIterator(rCtx, flagsBitSet, errorUpdates.iterator());
      });

      return asyncInvokeNext(ctx, command, stage);
   }

   private List<KeyValuePair<String, String>> computeErrorUpdatesAfterRemove(String key) {
      // List of proto schemas and their errors
      // Proto schemas with no errors have a null value
      List<KeyValuePair<String, String>> errorUpdates = new ArrayList<>();
      errorUpdates.add(KeyValuePair.of(key, null));

      StringBuilder sb = new StringBuilder();
      for (FileDescriptor fd : serializationContext.getFileDescriptors().values()) {
         String fdName = fd.getName();
         if (fd.isResolved()) {
            errorUpdates.add(KeyValuePair.of(fdName, null));
         } else {
            if (sb.length() > 0) {
               sb.append('\n');
            }
            sb.append(fdName);
            errorUpdates.add(KeyValuePair.of(fdName, "One of the imported files is missing or has errors"));
         }
      }
      errorUpdates.add(KeyValuePair.of("", sb.length() > 0 ? sb.toString() : null));
      return errorUpdates;
   }

   private InvocationStage updateSchemaErrorsIterator(InvocationContext ctx, long flagsBitSet,
                                                      Iterator<KeyValuePair<String, String>> iterator) {
      if (!iterator.hasNext())
         return InvocationStage.completedNullStage();

      KeyValuePair<String, String> keyErrorPair = iterator.next();
      Object errorsKey = keyErrorPair.getKey() + ERRORS_KEY_SUFFIX;
      String errorsValue = keyErrorPair.getValue();
      int segment = keyPartitioner.getSegment(errorsKey);
      WriteCommand writeCommand;
      if (errorsValue == null) {
         writeCommand = commandsFactory.buildRemoveCommand(errorsKey, null, segment, flagsBitSet);
      } else {
         PutKeyValueCommand put = commandsFactory.buildPutKeyValueCommand(errorsKey, errorsValue, segment,
                                                                          DEFAULT_METADATA, flagsBitSet);
         put.setPutIfAbsent(true);
         writeCommand = put;
      }
      InvocationStage stage = invoker.running().invokeStage(ctx, writeCommand);
      return stage.thenApplyMakeStage(ctx, writeCommand, (rCtx, rCommand, rv) -> updateSchemaErrorsIterator(rCtx, flagsBitSet, iterator));
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
      final Object key = command.getKey();
      final Object value = command.getNewValue();

      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      if (!(key instanceof String)) {
         throw log.keyMustBeString(key.getClass());
      }
      if (!(value instanceof String)) {
         throw log.valueMustBeString(value.getClass());
      }
      if (!shouldIntercept(key)) {
         return invokeNext(ctx, command);
      }
      if (!((String) key).endsWith(PROTO_KEY_SUFFIX)) {
         throw log.keyMustBeStringEndingWithProto(key);
      }

      // lock global errors key
      LockControlCommand cmd = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, command.getFlagsBitSet(),
                                                                       null);
      InvocationStage stage = invoker.running().invokeStage(ctx, cmd);

      return makeStage(asyncInvokeNext(ctx, command, stage)).thenApply(ctx, command, (rCtx, rCommand, rv) -> {
         if (rCommand.isSuccessful()) {
            long flagsBitSet = copyFlags(rCommand);
            if (rCtx.isOriginLocal()) {
               ProgressCallback progressCallback = new ProgressCallback();
               registerProtoFile((String) key, (String) value, progressCallback);

               List<KeyValuePair<String, String>> errorUpdates = computeErrorUpdates(progressCallback);
               return updateSchemaErrorsIterator(rCtx, flagsBitSet, errorUpdates.iterator());
            }
            registerProtoFile((String) key, (String) value, EMPTY_CALLBACK);
         }
         return InvocationStage.completedNullStage();
      });
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
      for (String fileName : serializationContext.getFileDescriptors().keySet()) {
         serializationContext.unregisterProtoFile(fileName);
      }

      return invokeNext(ctx, command);
   }

   private boolean shouldIntercept(Object key) {
      return !((String) key).endsWith(ERRORS_KEY_SUFFIX);
   }

   // --- unsupported operations: compute, computeIfAbsent, eval or any other functional map commands  ---

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) {
      return handleUnsupportedCommand(command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) {
      return handleUnsupportedCommand(command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) {
      return handleUnsupportedCommand(command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) {
      return handleUnsupportedCommand(command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) {
      return handleUnsupportedCommand(command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) {
      return handleUnsupportedCommand(command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) {
      return handleUnsupportedCommand(command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) {
      return handleUnsupportedCommand(command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) {
      return handleUnsupportedCommand(command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) {
      return handleUnsupportedCommand(command);
   }

   private Object handleUnsupportedCommand(ReplicableCommand command) {
      throw log.cacheDoesNotSupportCommand(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME, command.getClass().getName());
   }
}
