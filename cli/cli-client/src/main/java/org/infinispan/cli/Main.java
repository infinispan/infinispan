package org.infinispan.cli;

import org.infinispan.cli.shell.Shell;
import org.infinispan.cli.shell.ShellImpl;

/**
 * The CLI Shell
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class Main {

   /**
    * The
    *
    * @param args
    * @throws Exception
    */
   public static void main(String[] args) throws Exception {
      Shell shell = new ShellImpl();
      shell.init(args);
      shell.run();
      System.exit(0);
   }

}
