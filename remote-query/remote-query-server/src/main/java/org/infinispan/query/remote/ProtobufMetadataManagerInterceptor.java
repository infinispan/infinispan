package org.infinispan.query.remote;

import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.descriptors.FileDescriptor;
import org.infinispan.query.remote.logging.Log;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


/**
 * Intercepts updates to the protobuf schema file caches and updates the SerializationContext accordingly.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ProtobufMetadataManagerInterceptor extends BaseCustomInterceptor {

   private static final Log log = LogFactory.getLog(ProtobufMetadataManagerInterceptor.class, Log.class);

   /**
    * All error status keys end with this suffix.
    */
   public static final String ERRORS_KEY_SUFFIX = ".errors";

   /**
    * All protobuf definition source files end with this suffix.
    */
   public static final String PROTO_KEY_SUFFIX = ".proto";

   private CommandsFactory commandsFactory;

   private InterceptorChain invoker;

   private ProtobufMetadataManager protobufMetadataManager;

   private SerializationContext serializationContext;

   /**
    * A no-op callback.
    */
   private static final FileDescriptorSource.ProgressCallback EMPTY_CALLBACK = new FileDescriptorSource.ProgressCallback() {
      @Override
      public void handleError(String fileName, DescriptorParserException exception) {
      }

      @Override
      public void handleSuccess(String fileName) {
      }
   };

   private final class ProgressCallback implements FileDescriptorSource.ProgressCallback {

      private final InvocationContext ctx;

      private final Set<String> errorFiles = new TreeSet<String>();

      private ProgressCallback(InvocationContext ctx) {
         this.ctx = ctx;
      }

      public Set<String> getErrorFiles() {
         return errorFiles;
      }

      @Override
      public void handleError(String fileName, DescriptorParserException exception) {
         // handle first error per file, ignore the rest if any
         if (errorFiles.add(fileName)) {
            VisitableCommand cmd = commandsFactory.buildPutKeyValueCommand(fileName + ERRORS_KEY_SUFFIX, exception.getMessage(), null, null);
            invoker.invoke(ctx, cmd);
         }
      }

      @Override
      public void handleSuccess(String fileName) {
         VisitableCommand cmd = commandsFactory.buildRemoveCommand(fileName + ERRORS_KEY_SUFFIX, null, null);
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
            } catch (IOException e) {
               throw new CacheException("Failed to parse proto file : " + key, e);
            } catch (DescriptorParserException e) {
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
         } catch (IOException e) {
            throw new CacheException(e);
         } catch (DescriptorParserException e) {
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
            } catch (IOException e) {
               throw new CacheException("Failed to parse proto file : " + key, e);
            } catch (DescriptorParserException e) {
               throw new CacheException("Failed to parse proto file : " + key, e);
            }
         }
         return null;
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
         final String key = (String) command.getKey();
         if (shouldIntercept(key)) {
            serializationContext.unregisterProtoFile(key);
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
   public void init(CommandsFactory commandsFactory, InterceptorChain invoker, ProtobufMetadataManager protobufMetadataManager) {
      this.commandsFactory = commandsFactory;
      this.invoker = invoker;
      this.protobufMetadataManager = protobufMetadataManager;
      this.serializationContext = protobufMetadataManager.getSerializationContext();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      final Object result = invokeNextInterceptor(ctx, command);
      if (!ctx.isOriginLocal()) {
         // apply updates to the serialization context
         for (WriteCommand wc : command.getModifications()) {
            wc.acceptVisitor(ctx, serializationContextUpdaterVisitor);
         }
      }
      return result;
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
            if (!command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
               if (!((String) key).endsWith(PROTO_KEY_SUFFIX)) {
                  throw new CacheException("The key must end with \".proto\" : " + key);
               }

               // lock .errors key
               VisitableCommand cmd = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, null, null);
               invoker.invoke(ctx, cmd);
            }
         } else {
            return invokeNextInterceptor(ctx, command);
         }
      }

      final Object result = invokeNextInterceptor(ctx, command);

      if (command.isSuccessful()) {
         FileDescriptorSource source = new FileDescriptorSource()
               .addProtoFile((String) key, (String) value);

         ProgressCallback progressCallback = null;
         if (ctx.isOriginLocal() && !command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
            progressCallback = new ProgressCallback(ctx);
            source.withProgressCallback(progressCallback);
         } else {
            source.withProgressCallback(EMPTY_CALLBACK);
         }

         try {
            serializationContext.registerProtoFiles(source);
         } catch (IOException e) {
            throw new CacheException("Failed to parse proto file : " + key, e);
         } catch (DescriptorParserException e) {
            throw new CacheException("Failed to parse proto file : " + key, e);
         }

         if (progressCallback != null) {
            updateGlobalErrors(ctx, progressCallback.getErrorFiles());
         }
      }

      return result;
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
               throw new CacheException("The key must end with \".proto\" : " + key);
            }
            source.addProtoFile((String) key, (String) value);
         }
      }

      // lock .errors key
      VisitableCommand cmd = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, null, null);
      invoker.invoke(ctx, cmd);

      final Object result = invokeNextInterceptor(ctx, command);

      ProgressCallback progressCallback = null;
      if (ctx.isOriginLocal()) {
         progressCallback = new ProgressCallback(ctx);
         source.withProgressCallback(progressCallback);
      } else {
         source.withProgressCallback(EMPTY_CALLBACK);
      }

      try {
         serializationContext.registerProtoFiles(source);
      } catch (IOException e) {
         throw new CacheException(e);
      } catch (DescriptorParserException e) {
         throw new CacheException(e);
      }

      if (progressCallback != null) {
         updateGlobalErrors(ctx, progressCallback.getErrorFiles());
      }

      return result;
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
            VisitableCommand cmd = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, null, null);
            invoker.invoke(ctx, cmd);

            cmd = commandsFactory.buildRemoveCommand(key + ERRORS_KEY_SUFFIX, null, null);
            invoker.invoke(ctx, cmd);

            serializationContext.unregisterProtoFile(key);
            Map<String, FileDescriptor> fileDescriptors = serializationContext.getFileDescriptors();

            // put error key for all unresolved files and remove error key for all resolved files
            StringBuilder sb = new StringBuilder();
            for (FileDescriptor fd : fileDescriptors.values()) {
               if (fd.isResolved()) {
                  cmd = commandsFactory.buildRemoveCommand(fd.getName() + ERRORS_KEY_SUFFIX, null, null);
                  invoker.invoke(ctx, cmd);
               } else {
                  if (sb.length() > 0) {
                     sb.append('\n');
                  }
                  sb.append(fd.getName());
                  PutKeyValueCommand put = commandsFactory.buildPutKeyValueCommand(fd.getName() + ERRORS_KEY_SUFFIX, "One of the imported files is missing or has errors", null, null);
                  put.setPutIfAbsent(true);
                  invoker.invoke(ctx, put);
               }
            }

            if (sb.length() > 0) {
               cmd = commandsFactory.buildPutKeyValueCommand(ERRORS_KEY_SUFFIX, sb.toString(), null, null);
            } else {
               cmd = commandsFactory.buildRemoveCommand(ERRORS_KEY_SUFFIX, null, null);
            }
            invoker.invoke(ctx, cmd);
         }
      }

      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(final InvocationContext ctx, ReplaceCommand command) throws Throwable {
      final Object key = command.getKey();
      final Object value = command.getNewValue();

      if (ctx.isOriginLocal()) {
         if (!(key instanceof String)) {
            throw new CacheException("The key must be a string");
         }
         if (!(value instanceof String)) {
            throw new CacheException("The value must be a string");
         }
         if (!shouldIntercept(key)) {
            return invokeNextInterceptor(ctx, command);
         }
         if (!((String) key).endsWith(PROTO_KEY_SUFFIX)) {
            throw new CacheException("The key must end with \".proto\" : " + key);
         }

         // lock .errors key
         VisitableCommand cmd = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, null, null);
         invoker.invoke(ctx, cmd);

         final Object result = invokeNextInterceptor(ctx, command);

         if (command.isSuccessful()) {
            FileDescriptorSource source = new FileDescriptorSource()
                  .addProtoFile((String) key, (String) value);

            ProgressCallback progressCallback = null;
            if (ctx.isOriginLocal()) {
               progressCallback = new ProgressCallback(ctx);
               source.withProgressCallback(progressCallback);
            } else {
               source.withProgressCallback(EMPTY_CALLBACK);
            }

            try {
               serializationContext.registerProtoFiles(source);
            } catch (IOException e) {
               throw new CacheException("Failed to parse proto file : " + key, e);
            } catch (DescriptorParserException e) {
               throw new CacheException("Failed to parse proto file : " + key, e);
            }

            if (progressCallback != null) {
               updateGlobalErrors(ctx, progressCallback.getErrorFiles());
            }
         }

         return result;
      } else {
         return invokeNextInterceptor(ctx, command);
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      // lock .errors key
      VisitableCommand cmd = commandsFactory.buildLockControlCommand(ERRORS_KEY_SUFFIX, null, null);
      invoker.invoke(ctx, cmd);

      for (String fileName : serializationContext.getFileDescriptors().keySet()) {
         serializationContext.unregisterProtoFile(fileName);
      }

      return invokeNextInterceptor(ctx, command);
   }

   private boolean shouldIntercept(Object key) {
      return !((String) key).endsWith(ERRORS_KEY_SUFFIX);
   }

   private void updateGlobalErrors(InvocationContext ctx, Set<String> errorFiles) {
      // remove or update .errors accordingly
      VisitableCommand cmd;
      if (errorFiles.isEmpty()) {
         cmd = commandsFactory.buildRemoveCommand(ERRORS_KEY_SUFFIX, null, null);
      } else {
         StringBuilder sb = new StringBuilder();
         for (String fileName : errorFiles) {
            if (sb.length() > 0) {
               sb.append('\n');
            }
            sb.append(fileName);
         }
         cmd = commandsFactory.buildPutKeyValueCommand(ERRORS_KEY_SUFFIX, sb.toString(), null, null);
      }
      invoker.invoke(ctx, cmd);
   }
}
