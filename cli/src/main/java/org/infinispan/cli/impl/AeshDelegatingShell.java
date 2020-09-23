package org.infinispan.cli.impl;

import org.aesh.readline.ShellImpl;
import org.aesh.terminal.Connection;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class AeshDelegatingShell extends ShellImpl {
   private final Connection connection;

   public AeshDelegatingShell(Connection connection) {
      super(connection);
      this.connection = connection;
   }

   public Connection getConnection() {
      return connection;
   }
}
