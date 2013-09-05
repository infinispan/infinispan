package org.infinispan.interceptors;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;

import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;

/**
 * Interceptor to verify whether parameters passed into cache are marshallables
 * or not. This is handy in situations where we want to find out before
 * marshalling whether the type of object is marshallable. Such situations
 * include lazy deserialization, or when marshalling happens in a separate
 * thread and marshalling failures might be swallowed. </p>
 *
 * This interceptor offers the possibility to discover these issues way before
 * the code has moved onto a different thread where it's harder to communicate
 * with the original request thread.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public class IsMarshallableInterceptor extends CommandInterceptor {

   private StreamingMarshaller marshaller;
   private DistributionManager distManager;
   private boolean storeAsBinary;

   private static final Log log = LogFactory.getLog(IsMarshallableInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   protected void injectMarshaller(@ComponentName(CACHE_MARSHALLER) StreamingMarshaller marshaller,
                                   DistributionManager distManager) {
      this.marshaller = marshaller;
      this.distManager = distManager;
   }

   @Start
   protected void start() {
      storeAsBinary = cacheConfiguration.storeAsBinary().enabled()
            && (cacheConfiguration.storeAsBinary().storeKeysAsBinary()
                      || cacheConfiguration.storeAsBinary().storeValuesAsBinary());
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      Object key = command.getKey();
      if (isStoreAsBinary() || getMightGoRemote(ctx, key, command))
         checkMarshallable(key);
      return super.visitGetKeyValueCommand(ctx, command);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (isStoreAsBinary() || isClusterInvocation(ctx, command))
         checkMarshallable(command.getKeys());
      return super.visitLockControlCommand(ctx, command);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (isStoreAsBinary() || isClusterInvocation(ctx, command) || isStoreInvocation(command))
         checkMarshallable(command.getKey(), command.getValue());
      return super.visitPutKeyValueCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (isStoreAsBinary() || isClusterInvocation(ctx, command) || isStoreInvocation(command))
         checkMarshallable(command.getMap());
      return super.visitPutMapCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      if (isStoreAsBinary() || isClusterInvocation(ctx, command) || isStoreInvocation(command))
         checkMarshallable(command.getKey());
      return super.visitRemoveCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      if (isStoreAsBinary() || isClusterInvocation(ctx, command) || isStoreInvocation(command))
         checkMarshallable(command.getKey(), command.getNewValue());
      return super.visitReplaceCommand(ctx, command);
   }

   private boolean isClusterInvocation(InvocationContext ctx, FlagAffectedCommand command) {
      // If the cache is local, the interceptor should only be enabled in case
      // of lazy deserialization or when an async store is in place. So, if
      // any cache store is configured, check whether it'll be skipped
      return ctx.isOriginLocal()
            && cacheConfiguration.clustering().cacheMode().isClustered()
            && !command.hasFlag(Flag.CACHE_MODE_LOCAL);
   }

   private boolean isStoreInvocation(FlagAffectedCommand command) {
      // If the cache is local, the interceptor should only be enabled in case
      // of lazy deserialization or when an async store is in place. So, if
      // any cache store is configured, check whether it'll be skipped
      return !cacheConfiguration.clustering().cacheMode().isClustered()
            && !cacheConfiguration.persistence().stores().isEmpty()
            && !command.hasFlag(Flag.SKIP_CACHE_STORE);
   }

   private boolean isStoreAsBinary() {
      return storeAsBinary;
   }

   private boolean getMightGoRemote(InvocationContext ctx, Object key, FlagAffectedCommand command) {
      return ctx.isOriginLocal()
            && cacheConfiguration.clustering().cacheMode().isDistributed()
            && !command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)
            && !command.hasFlag(Flag.IGNORE_RETURN_VALUES)
            && !distManager.getLocality(key).isLocal();
   }

   private void checkMarshallable(Object... objs) throws NotSerializableException {
      for (Object o : objs) {
         boolean marshallable = false;
         try {
            marshallable = marshaller.isMarshallable(o);
         } catch (Exception e) {
            throwNotSerializable(o, e);
         }

         if (!marshallable)
            throwNotSerializable(o, null);
      }
   }

   private void throwNotSerializable(Object o, Throwable t) {
      String msg = String.format(
            "Object of type %s expected to be marshallable", o.getClass());
      if (t == null)
         throw new NotSerializableException(msg);
      else
         throw new NotSerializableException(msg, t);
   }

   private void checkMarshallable(Map<Object, Object> objs) throws NotSerializableException {
      for (Map.Entry<Object, Object> entry : objs.entrySet())
         checkMarshallable(entry.getKey(), entry.getValue());
   }

}
