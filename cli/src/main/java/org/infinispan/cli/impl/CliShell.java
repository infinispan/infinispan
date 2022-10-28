package org.infinispan.cli.impl;

import java.io.IOException;
import java.nio.charset.Charset;

import org.aesh.readline.tty.terminal.TerminalConnection;

/**
 * @since 14.0
 **/
public class CliShell extends AeshDelegatingShell {
   public CliShell() throws IOException {
      super(new TerminalConnection(Charset.defaultCharset(), System.in, System.out));
   }
}
