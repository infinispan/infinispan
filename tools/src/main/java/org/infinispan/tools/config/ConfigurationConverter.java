package org.infinispan.tools.config;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.infinispan.Version;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;

/**
 * ConfigurationConverter.
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class ConfigurationConverter {

   public static void convert(InputStream is, OutputStream os) throws Exception {
      ParserRegistry registry = new ParserRegistry();
      ConfigurationBuilderHolder configHolder = registry.parse(is);
      Map<String, Configuration> configurations = new HashMap<>();
      for(Entry<String, ConfigurationBuilder> config : configHolder.getNamedConfigurationBuilders().entrySet()) {
         configurations.put(config.getKey(), config.getValue().build());
      }
      registry.serialize(os, configHolder.getGlobalConfigurationBuilder().build(), configurations);
   }

   public static final void main(String args[]) throws Exception {
      InputStream is = null;
      OutputStream os = null;
      switch(args.length) {
      case 0:
         is = System.in;
         os = System.out;
         break;
      case 1:
         is = new FileInputStream(args[0]);
         os = System.out;
         break;
      case 2:
         is = new FileInputStream(args[0]);
         os = new FileOutputStream(args[1]);
         break;
      default:
         System.err.printf("Infinispan configuration converter v%s\n", Version.getVersion());
         System.err.println("Usage: converter [input [output]]");
         System.exit(1);
      }
      convert(is, os);
   }

}
