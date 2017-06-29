package org.infinispan.query.remote.impl;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.FileDescriptor;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;


/**
 * Intercepts updates to the protobuf schema file caches and updates the SerializationContext accordingly.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class ProtobufMetadataManagerInterceptor extends BaseCustomAsyncInterceptor implements ProtobufMetadataManagerConstants {
   private static final Metadata DEFAULT_METADATA = new EmbeddedMetadata.Builder().build();

   private CommandsFactory commandsFactory;

   private AsyncInterceptorChain invoker;

   private SerializationContext serializationContext;

   /**
    * A no-op callback.
    */
   private static final FileDescriptorSource.ProgressCallback EMPTY_CALLBACK = new FileDescriptorSource.ProgressCallback() {
   };

   private final class ProgressCallback implements FileDescriptorSource.ProgressCallback {

      private final InvocationContext ctx;
      private final long flagsBitSet;

      private final Set<String> errorFiles = new TreeSet<>();

      private ProgressCallback(InvocationContext ctx, long flagsBitSet) {
         this.ctx = ctx;
         this.flagsBitSet = flagsBitSet;
      }

      Set<String> getErrorFiles() {
         return errorFiles;
      }

      @Override
      public void handleError(String fileName, DescriptorParserException exception) {
         // handle first error per file, ignore the rest if any
         if (errorFiles.add(fileName)) {
            VisitableCommand cmd = commandsFactory.buildPutKeyValueCommand(fileName + ERRORS_KEY_SUFFIX, exception.getMessage(), DEFAULT_METADATA, flagsBitSet);
            invoker.invoke(ctx, cmd);
         }
      }

      @Override
      public void handleSuccess(String fileName) {
         VisitableCommand cmd = commandsFactory.buildRemoveCommand(fileName + ERRORS_KEY_SUFFIX, null, flagsBitSet);
         invoker.invoke(ctx, cmd);
      }
   }

   /**
    * Visitor used for handling the list of modifications of a PrepareCommand.
    */
   private final AbstractVisitor serializationContextUpdaterVisitor = new AbstractVisitor() {

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         final String key = (String) command.getKey();
         if (shouldIntercept(key)) {
            FileDescriptorSource source = new FileDescriptorSource()
                  .withProgressCallback(EMPTY_CALLBACK)
                  .addProtoFile(key, (String) command.getValue());
            try {
               serializationContext.registerProtoFiles(source);
            } catch (IOException | DescriptorParserException e) {
               throw new CacheException("Failed to parse proto file : " + key, e);
            }
         }
         return null;
      }

      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
         final Map<Object, Object> map = command.getMap();
         FileDescriptorSource source = new FileDescriptorSource()
               .withProgressCallback(EMPTY_CALLBACK);
         for (Object key : map.keySet()) {
            if (shouldIntercept(key)) {
               source.addProtoFile((String) key, (String) map.get(key));
            }
         }
         try {
            serializationContext.registerProtoFiles(source);
         } catch (IOException | DescriptorParserException e) {
            throw new CacheException(e);
         }
         return null;
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
         final String key = (String) command.getKey();
         if (shouldIntercept(key)) {
            FileDescriptorSource source = new FileDescriptorSource()
                  .withProgressCallback(EMPTY_CALLBACK)
                  .addProtoFile(key, (String) command.getNewValue());
            try {
               serializationContext.registerProtoFiles(source);
            } catch (IOException | DescriptorParserException e) {
               throw new CacheException("Failed to parse proto file : " + key, e);
            }
         }
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         final String key = (String) command.getKey();
         if (shouldIntercept(key)) {
            if (serializationContext.getFileDescriptors().containsKey(key)) {
               serializationContext.unregisterProtoFile(key);
            }
         }
         return null;
      }

      @Override
      public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
         for (String fileName : serializationContext.getFileDescriptors().keySet()) {
            serializationContext.unregisterProtoFile(fileName);
         }
         return null;
      }
   };

   @Inject
   public void init(CommandsFactory commandsFactory, AsyncInterceptorChain invoker, ProtobufMetadataManager protobufMetadataManager) {
      this.commandsFactory = commandsFactory;
      this.invoker = invoker;
      this.serializationContext = ((ProtobufMetadataManagerImpl) protobufMetadataManager).getSerializationContext();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         if (!rCtx.isOriginLocal()) {
            // apply updates to the serialization context
            for (WriteCommand wc : ((PrepareCommand) rCommand).getModifications()) {
               wc.acceptVisitor(rCtx, serializationContextUpdaterVisitor);
            }
         }
      });
   }

   @Override
   public Object visitPutKeyValueCommand(final InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      final Object key = command.getKey();
      final Object value = command.getValue();

      if (ctx.isOriginLocal()) {
         if (!(key instanceof String)) {
            throw new CacheException("The key must be a string");
         }
         if (!(value instanceof String)) {
            throw new CacheException("The value must be a string");
         }
         if (shouldIntercept(key)) {
            if (!command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER | FlagBitSets.SKIP_LOCKING)) {
               if (!((String) key).endsWith(PROTO_KEY_SUFFIX)) {
                  throw new CacheException("The key must be a string ending with \".proto\" : " + key);
               }

               // lock .errors key
               VisitableCommand cmd = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, command.getFlagsBitSet(), null);
               invoker.invoke(ctx, cmd);
            }
         } else {
            return invokeNext(ctx, command);
         }
      }

      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         PutKeyValueCommand putKeyValueCommand = (PutKeyValueCommand) rCommand;
         if (putKeyValueCommand.isSuccessful()) {
            FileDescriptorSource source = new FileDescriptorSource()
                  .addProtoFile((String) key, (String) value);

            long flagsBitSet = copyFlags(putKeyValueCommand);
            ProgressCallback progressCallback = null;
            if (rCtx.isOriginLocal() && !putKeyValueCommand.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
               progressCallback = new ProgressCallback(rCtx, flagsBitSet);
               source.withProgressCallback(progressCallback);
            } else {
               source.withProgressCallback(EMPTY_CALLBACK);
            }

            try {
               serializationContext.registerProtoFiles(source);
            } catch (IOException | DescriptorParserException e) {
               throw new CacheException("Failed to parse proto file : " + key, e);
            }

            if (progressCallback != null) {
               updateGlobalErrors(rCtx, progressCallback.getErrorFiles(), flagsBitSet);
            }
         }
      });
   }

   /**
    * For preload, we need to copy the CACHE_MODE_LOCAL flag from the put command.
    * But we also need to remove the SKIP_CACHE_STORE flag, so that existing .errors keys are updated.
    */
   private long copyFlags(FlagAffectedCommand command) {
      return EnumUtil.diffBitSets(command.getFlagsBitSet(), FlagBitSets.SKIP_CACHE_STORE);
   }

   @Override
   public Object visitPutMapCommand(final InvocationContext ctx, PutMapCommand command) throws Throwable {
      final Map<Object, Object> map = command.getMap();

      FileDescriptorSource source = new FileDescriptorSource();
      for (Object key : map.keySet()) {
         final Object value = map.get(key);
         if (!(key instanceof String)) {
            throw new CacheException("The key must be a string");
         }
         if (!(value instanceof String)) {
            throw new CacheException("The value must be a string");
         }
         if (shouldIntercept(key)) {
            if (!((String) key).endsWith(PROTO_KEY_SUFFIX)) {
               throw new CacheException("The key must be a string ending with \".proto\" : " + key);
            }
            source.addProtoFile((String) key, (String) value);
         }
      }

      // lock .errors key
      VisitableCommand cmd = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, command.getFlagsBitSet(), null);
      invoker.invoke(ctx, cmd);

      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         long flagsBitSet = copyFlags(((PutMapCommand) rCommand));
         ProgressCallback progressCallback = null;
         if (rCtx.isOriginLocal()) {
            progressCallback = new ProgressCallback(rCtx, flagsBitSet);
            source.withProgressCallback(progressCallback);
         } else {
            source.withProgressCallback(EMPTY_CALLBACK);
         }

         try {
            serializationContext.registerProtoFiles(source);
         } catch (IOException | DescriptorParserException e) {
            throw new CacheException(e);
         }

         if (progressCallback != null) {
            updateGlobalErrors(rCtx, progressCallback.getErrorFiles(), flagsBitSet);
         }
      });
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         if (!(command.getKey() instanceof String)) {
            throw new CacheException("The key must be a string");
         }
         String key = (String) command.getKey();
         if (shouldIntercept(key)) {
            // lock .errors key
            long flagsBitSet = copyFlags(command);
            VisitableCommand cmd = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, flagsBitSet, null);
            invoker.invoke(ctx, cmd);

            cmd = commandsFactory.buildRemoveCommand(key + ERRORS_KEY_SUFFIX, null, flagsBitSet);
            invoker.invoke(ctx, cmd);

            if (serializationContext.getFileDescriptors().containsKey(key)) {
               serializationContext.unregisterProtoFile(key);
            }

            // put error key for all unresolved files and remove error key for all resolved files
            StringBuilder sb = new StringBuilder();
            for (FileDescriptor fd : serializationContext.getFileDescriptors().values()) {
               if (fd.isResolved()) {
                  cmd = commandsFactory.buildRemoveCommand(fd.getName() + ERRORS_KEY_SUFFIX, null, flagsBitSet);
                  invoker.invoke(ctx, cmd);
               } else {
                  if (sb.length() > 0) {
                     sb.append('\n');
                  }
                  sb.append(fd.getName());
                  PutKeyValueCommand put = commandsFactory.buildPutKeyValueCommand(fd.getName() + ERRORS_KEY_SUFFIX, "One of the imported files is missing or has errors", DEFAULT_METADATA, flagsBitSet);
                  put.setPutIfAbsent(true);
                  invoker.invoke(ctx, put);
               }
            }

            if (sb.length() > 0) {
               cmd = commandsFactory.buildPutKeyValueCommand(ERRORS_KEY_SUFFIX, sb.toString(), DEFAULT_METADATA, flagsBitSet);
            } else {
               cmd = commandsFactory.buildRemoveCommand(ERRORS_KEY_SUFFIX, null, flagsBitSet);
            }
            invoker.invoke(ctx, cmd);
         }
      }

      return invokeNext(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(final InvocationContext ctx, ReplaceCommand command) throws Throwable {
      final Object key = command.getKey();
      final Object value = command.getNewValue();

      if (!ctx.isOriginLocal()) {
         return invokeNext(ctx, command);
      }
      if (!(key instanceof String)) {
         throw new CacheException("The key must be a string");
      }
      if (!(value instanceof String)) {
         throw new CacheException("The value must be a string");
      }
      if (!shouldIntercept(key)) {
         return invokeNext(ctx, command);
      }
      if (!((String) key).endsWith(PROTO_KEY_SUFFIX)) {
         throw new CacheException("The key must be a string ending with \".proto\" : " + key);
      }

      // lock .errors key
      VisitableCommand cmd = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, command.getFlagsBitSet(), null);
      invoker.invoke(ctx, cmd);

      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         if (rCommand.isSuccessful()) {
            FileDescriptorSource source = new FileDescriptorSource()
                        .addProtoFile((String) key, (String) value);

            long flagsBitSet = copyFlags(((WriteCommand) rCommand));
            ProgressCallback progressCallback = null;
            if (rCtx.isOriginLocal()) {
               progressCallback = new ProgressCallback(rCtx, flagsBitSet);
               source.withProgressCallback(progressCallback);
            } else {
               source.withProgressCallback(EMPTY_CALLBACK);
            }

            try {
               serializationContext.registerProtoFiles(source);
            } catch (IOException | DescriptorParserException e) {
               throw new CacheException("Failed to parse proto file : " + key, e);
            }

            if (progressCallback != null) {
               updateGlobalErrors(rCtx, progressCallback.getErrorFiles(), flagsBitSet);
            }
         }
      });
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      for (String fileName : serializationContext.getFileDescriptors().keySet()) {
         serializationContext.unregisterProtoFile(fileName);
      }

      return invokeNext(ctx, command);
   }

   private boolean shouldIntercept(Object key) {
      return !((String) key).endsWith(ERRORS_KEY_SUFFIX);
   }

   private void updateGlobalErrors(InvocationContext ctx, Set<String> errorFiles, long flagsBitSet) {
      // remove or update .errors accordingly
      VisitableCommand cmd;
      if (errorFiles.isEmpty()) {
         cmd = commandsFactory.buildRemoveCommand(ERRORS_KEY_SUFFIX, null, flagsBitSet);
      } else {
         StringBuilder sb = new StringBuilder();
         for (String fileName : errorFiles) {
            if (sb.length() > 0) {
               sb.append('\n');
            }
            sb.append(fileName);
         }
         cmd = commandsFactory.buildPutKeyValueCommand(ERRORS_KEY_SUFFIX, sb.toString(), DEFAULT_METADATA, flagsBitSet);
      }
      invoker.invoke(ctx, cmd);
   }
}
