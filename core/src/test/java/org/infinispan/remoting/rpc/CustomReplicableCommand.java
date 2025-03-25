package org.infinispan.remoting.rpc;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
public class CustomReplicableCommand implements CacheRpcCommand, VisitableCommand {

   final ByteString cacheName;
   final Object arg;
   Address origin;

   public CustomReplicableCommand(ByteString cacheName, Object arg) {
      this.cacheName = cacheName;
      this.arg = arg;
   }

   @ProtoFactory
   CustomReplicableCommand(ByteString cacheName, MarshallableObject<?> arg) {
      this(cacheName, MarshallableObject.unwrap(arg));
   }

   @Override
   public Object invoke() throws Throwable {
      if (arg instanceof Throwable) {
         throw (Throwable) arg;
      }

      // echo the arg back to the caller
      return arg;
   }

   @Override
   @ProtoField(1)
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public void setOrigin(Address origin) {
      this.origin = origin;
   }

   @Override
   public Address getOrigin() {
      return origin;
   }

   @ProtoField(2)
   MarshallableObject<?> getArg() {
      return MarshallableObject.create(arg);
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
