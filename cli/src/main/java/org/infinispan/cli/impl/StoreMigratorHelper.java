package org.infinispan.cli.impl;

import java.util.Properties;

import org.infinispan.tools.store.migrator.StoreMigrator;

/**
 * @since 15.0
 **/
public class StoreMigratorHelper {
   public static void run(Properties props, boolean verbose) throws Exception {
      new StoreMigrator(props).run();
   }
}
