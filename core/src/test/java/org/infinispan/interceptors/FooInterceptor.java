package org.infinispan.interceptors;

import org.infinispan.interceptors.base.BaseCustomInterceptor;

public class FooInterceptor extends BaseCustomInterceptor {
   private String foo;

   public String getFoo() {
      return foo;
   }

   public void setFoo(String foo) {
      this.foo = foo;
   }

}
