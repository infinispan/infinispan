package org.infinispan.cli.printers;

import org.aesh.command.shell.Shell;

/**
 * @since 14.0
 **/
public abstract class AbstractPrettyPrinter implements PrettyPrinter {
   protected final Shell shell;

   protected AbstractPrettyPrinter(Shell shell) {
      this.shell = shell;
   }
}
