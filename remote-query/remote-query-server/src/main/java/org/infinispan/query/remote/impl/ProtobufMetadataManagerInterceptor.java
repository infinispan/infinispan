package org.infinispan.query.remote.impl;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTO_KEY_SUFFIX;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
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
import org.infinispan.commons.internal.InternalCacheNames;
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

   private static final class ProgressCallback implements FileDescriptorSource.ProgressCallback {
      // keep them sorted by file name
      private final Map<String, DescriptorParserException> fileStatus = new TreeMap<>();

      Map<String, DescriptorParserException> getErrorFiles() {
         return fileStatus.entrySet().stream().filter(e -> e.getValue() != null)
               .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }

      Set<String> getSuccessFiles() {
         return fileStatus.entrySet().stream().filter(e -> e.getValue() == null).map(Map.Entry::getKey)
               .collect(Collectors.toSet());
      }

      @Override
      public void handleError(String fileName, DescriptorParserException exception) {
         // keep the first error per file, ignore all following errors as they are most likely consequences of the first
         fileStatus.put(fileName, exception);
      }

      @Override
      public void handleSuccess(String fileName) {
         // a file that is already failed cannot be added as success
         fileStatus.putIfAbsent(fileName, null);
      }
   }

   /**
    * Visitor used for handling the list of modifications of a PrepareCommand.
    */
   private final AbstractVisitor serializationContextUpdaterVisitor = new AbstractVisitor() {

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
         registerSingleProtoFile(command.getKey(), command.getValue());
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
         final Map<Object, Object> map = command.getMap();
         FileDescriptorSource source = new FileDescriptorSource().withProgressCallback(EMPTY_CALLBACK);
         for (Object key : map.keySet()) {
            var protoKey = validateKey(key);
            if (isErrorKeySuffix(protoKey)) {
               continue;
            }
            var value = validateValue(map.get(key));
            log.debugf("Registering proto file '%s': %s", protoKey, value);
            source.addProtoFile(protoKey, value);
         }
         registerFileDescriptorSource(source, source.getFiles().keySet().toString());
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
         registerSingleProtoFile(command.getKey(), command.getNewValue());
         return Boolean.TRUE;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
         var key = validateKey(command.getKey());
         if (isErrorKeySuffix(key)) {
            return null;
         }
         validateKeySuffix(key);
         removeProtoFile(key);
         return null;
      }

      private void registerSingleProtoFile(Object key, Object value) {
         var protoKey = validateKey(key);
         if (isErrorKeySuffix(protoKey)) {
            return;
         }
         validateKeySuffix(protoKey);
         registerProtoFile(protoKey, validateValue(value), EMPTY_CALLBACK);
      }
   };

   private void registerProtoFile(String name, String content, FileDescriptorSource.ProgressCallback callback) {
      log.debugf("Registering proto file '%s': %s", name, content);
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
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      var key = validateKey(command.getKey());

      if (isErrorKeySuffix(key)) {
         return invokeNext(ctx, command);
      }
      validateKeySuffix(key);

      InvocationStage stage;

      if (!command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER | FlagBitSets.SKIP_LOCKING)) {
         // lock global errors key
         stage = invoker.running().invokeStage(ctx, buildLockCommand(command.getFlagsBitSet()));
      } else {
         stage = SyncInvocationStage.completedNullStage();
      }

      return makeStage(asyncInvokeNext(ctx, command, stage))
            .thenApply(ctx, command, this::handlePutKeyValueResult);
   }

   private InvocationStage handlePutKeyValueResult(InvocationContext ctx, PutKeyValueCommand cmd, Object rv) {
      assert ctx.isOriginLocal();
      if (cmd.isSuccessful()) {
         var key = validateKey(cmd.getKey());
         var value = validateValue(cmd.getValue());

         if (cmd.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            registerProtoFile(key, value, EMPTY_CALLBACK);
            return makeStage(rv);
         }
         return handleLocalProtoFileRegister(ctx, key, value, copyFlags(cmd));
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
         if (!sb.isEmpty()) {
            sb.append('\n');
         }
         sb.append(fdName);
         errorUpdates.add(KeyValuePair.of(fdName, errorValue));
      }

      for (String successKeyName : progressCallback.getSuccessFiles()) {
         errorUpdates.add(KeyValuePair.of(successKeyName, null));
      }
      errorUpdates.add(KeyValuePair.of("", !sb.isEmpty() ? sb.toString() : null));
      return errorUpdates;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }

      // lock global errors key
      InvocationStage stage = invoker.running().invokeStage(ctx, buildLockCommand(command.getFlagsBitSet()));
      return makeStage(asyncInvokeNext(ctx, command, stage)).thenApply(ctx, command, this::handleLocalPutMapResult);
   }

   private InvocationStage handleLocalPutMapResult(InvocationContext ctx, PutMapCommand cmd, Object ignored) {
      assert ctx.isOriginLocal();
      FileDescriptorSource source = new FileDescriptorSource();
      for (Object key : cmd.getMap().keySet()) {
         var protoKey = validateKey(key);
         var value = validateValue(cmd.getMap().get(key));
         if (isErrorKeySuffix(protoKey)) {
            continue;
         }
         validateKeySuffix(protoKey);
         log.debugf("Registering proto file '%s': %s", protoKey, value);
         source.addProtoFile(protoKey, value);
      }
      var callback = new ProgressCallback();
      source.withProgressCallback(callback);
      registerFileDescriptorSource(source, source.getFiles().keySet().toString());
      List<KeyValuePair<String, String>> errorUpdates = computeErrorUpdates(callback);
      return updateSchemaErrorsIterator(ctx, copyFlags(cmd), errorUpdates.iterator());
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
      var key = validateKey(command.getKey());
      if (isErrorKeySuffix(key)) {
         return invokeNext(ctx, command);
      }
      // lock global errors key
      InvocationStage stage = invoker.running().invokeStage(ctx, buildLockCommand(copyFlags(command)));
      stage = stage.thenApplyMakeStage(ctx, command, (rCtx, rCommand, __) -> {
         removeProtoFile(validateKey(rCommand.getKey()));
         // put error key for all unresolved files and remove error key for all resolved files
         // Error keys to be removed have a null value
         List<KeyValuePair<String, String>> errorUpdates = computeErrorUpdatesAfterRemove(key);
         return updateSchemaErrorsIterator(rCtx, copyFlags(rCommand), errorUpdates.iterator());
      });

      return asyncInvokeNext(ctx, command, stage);
   }

   private void removeProtoFile(String key) {
      if (serializationContext.getFileDescriptors().containsKey(key)) {
         serializationContext.unregisterProtoFile(key);
      }
      if (serializationContextRegistry.getUserCtx().getFileDescriptors().containsKey(key)) {
         serializationContextRegistry.removeProtoFile(SerializationContextRegistry.MarshallerType.USER, key);
      }
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
            if (!sb.isEmpty()) {
               sb.append('\n');
            }
            sb.append(fdName);
            errorUpdates.add(KeyValuePair.of(fdName, "One of the imported files is missing or has errors"));
         }
      }
      errorUpdates.add(KeyValuePair.of("", !sb.isEmpty() ? sb.toString() : null));
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
      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      var key = validateKey(command.getKey());
      if (isErrorKeySuffix(key)) {
         return invokeNext(ctx, command);
      }
      validateKeySuffix(key);

      // lock global errors key
      InvocationStage stage = invoker.running().invokeStage(ctx, buildLockCommand(copyFlags(command)));

      return makeStage(asyncInvokeNext(ctx, command, stage))
            .thenApply(ctx, command, (rCtx, rCommand, rv) -> {
               assert rCtx.isOriginLocal();
               return rCommand.isSuccessful() ?
                     handleLocalProtoFileRegister(rCtx, validateKey(rCommand.getKey()), validateValue(rCommand.getNewValue()), copyFlags(rCommand))
                           .thenReturn(rCtx, rCommand, Boolean.TRUE) :
                     InvocationStage.completedFalseStage();
            });
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) {
      for (String fileName : serializationContext.getFileDescriptors().keySet()) {
         serializationContext.unregisterProtoFile(fileName);
      }
      for (var name : serializationContextRegistry.getUserCtx().getFileDescriptors().keySet()) {
         serializationContextRegistry.removeProtoFile(SerializationContextRegistry.MarshallerType.USER, name);
      }

      return invokeNext(ctx, command);
   }

   // --- unsupported operations: compute, computeIfAbsent, eval or any other functional map commands  ---

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) {
      if (command.hasAnyFlag(FlagBitSets.ROLLING_UPGRADE)) {
         return invokeNextThenApply(ctx, command, this::handleComputeCommandResult);
      }
      return handleUnsupportedCommand(command);
   }

   private Object handleComputeCommandResult(InvocationContext ctx, ComputeCommand cmd, Object rv) {
      if (cmd.isSuccessful()) {
         var key = validateKey(cmd.getKey());
         var value = validateValue(rv);
         if (ctx.isOriginLocal()) {
            return handleLocalProtoFileRegister(ctx, key, value, 0);
         }
         registerProtoFile(key, value, EMPTY_CALLBACK);
      }
      return rv;
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

   private static Object handleUnsupportedCommand(ReplicableCommand command) {
      throw log.cacheDoesNotSupportCommand(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME, command.getClass().getName());
   }

   private InvocationStage handleLocalProtoFileRegister(InvocationContext ctx, String key, String value, long flags) {
      ProgressCallback progressCallback = new ProgressCallback();
      registerProtoFile(key, value, progressCallback);
      List<KeyValuePair<String, String>> errorUpdates = computeErrorUpdates(progressCallback);
      return updateSchemaErrorsIterator(ctx, flags, errorUpdates.iterator());
   }

   private LockControlCommand buildLockCommand(long flags) {
      return commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, flags, null);
   }

   private static String validateKey(Object key) {
      if (key instanceof String) {
         return (String) key;
      }
      throw log.keyMustBeString(key.getClass());
   }

   private static String validateValue(Object value) {
      if (value instanceof String) {
         return (String) value;
      }
      throw log.valueMustBeString(value.getClass());
   }

   private static void validateKeySuffix(String key) {
      if (!key.endsWith(PROTO_KEY_SUFFIX)) {
         throw log.keyMustBeStringEndingWithProto(key);
      }
   }

   private static boolean isErrorKeySuffix(String key) {
      return key.endsWith(ERRORS_KEY_SUFFIX);
   }

   /**
    * For preload, we need to copy the CACHE_MODE_LOCAL flag from the put command.
    * But we also need to remove the SKIP_CACHE_STORE flag, so that existing .errors keys are updated.
    */
   private static long copyFlags(FlagAffectedCommand command) {
      return command.getFlagsBitSet() | FlagBitSets.IGNORE_RETURN_VALUES & ~FlagBitSets.SKIP_CACHE_STORE;
   }
}
