package org.infininspan.quarkus.server;

import org.infinispan.server.Bootstrap;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;

@QuarkusMain
public class InfinispanQuarkusServer implements QuarkusApplication {

   @Override
   public int run(String... args) {
      Bootstrap.main(args);
      return 0;
   }
}
