package org.infinispan.interceptors;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.BaseCustomInterceptor;

import java.util.concurrent.atomic.AtomicBoolean;

public class FooInterceptor extends BaseCustomInterceptor {

   public final AtomicBoolean putInvoked = new AtomicBoolean(false);

   private String foo;

   public String getFoo() {
      return foo;
   }

   public void setFoo(String foo) {
      this.foo = foo;
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      putInvoked.set(true);
      return super.visitPutKeyValueCommand(ctx, command);
   }
}
