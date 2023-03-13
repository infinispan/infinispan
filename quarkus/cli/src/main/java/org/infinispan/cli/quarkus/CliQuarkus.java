package org.infinispan.cli.quarkus;

import java.io.IOException;

import org.infinispan.cli.commands.CLI;

import io.quarkus.runtime.annotations.QuarkusMain;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@QuarkusMain
public class CliQuarkus {
   public static void main(String[] args) throws IOException {
      CLI.main(args);
   }
}
