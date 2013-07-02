package org.infinispan.cli.shell;

import java.io.IOException;

import org.infinispan.cli.Context;


/**
 *
 * Shell.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface Shell {

   void init(String[] args) throws Exception;

   void run() throws IOException;

   String getCWD();

   String renderColor(Color c, String toColorize);

   Context getContext();

}
