package org.infinispan.cli.impl;

import java.io.OutputStream;

import org.aesh.command.shell.Shell;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class ShellOutputStreamAdapter extends OutputStream {
   final Shell shell;

   public ShellOutputStreamAdapter(Shell shell) {
      this.shell = shell;
   }

   @Override
   public void write(int b) {
      shell.write(Character.toString((char)b));
   }
}
