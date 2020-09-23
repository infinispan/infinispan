package org.infinispan.cli.impl;

import java.io.InputStream;

import org.aesh.command.settings.ManProvider;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CliManProvider implements ManProvider {
   @Override
   public InputStream getManualDocument(String commandName) {
      return this.getClass().getClassLoader().getResourceAsStream("help/" + commandName + ".adoc");
   }
}
