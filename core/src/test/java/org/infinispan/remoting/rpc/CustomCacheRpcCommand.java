package org.infinispan.remoting.rpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.ByteString;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
public class CustomCacheRpcCommand extends BaseRpcCommand implements VisitableCommand {

   final Object arg;

   @ProtoFactory
   CustomCacheRpcCommand(ByteString cacheName, MarshallableObject<?> arg) {
      this(cacheName, MarshallableObject.unwrap(arg));
   }

   CustomCacheRpcCommand(ByteString cacheName, Object arg) {
      super(cacheName);
      this.arg = arg;
   }

   @ProtoField(2)
   MarshallableObject<?> getArg() {
      return MarshallableObject.create(arg);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      if (arg instanceof Throwable) {
         throw (Throwable) arg;
      }

      // echo the arg back to the caller
      return CompletableFuture.completedFuture(arg);
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitUnknownCommand(ctx, this);
   }

   @Override
   public LoadType loadType() {
      throw new UnsupportedOperationException();
   }
}
