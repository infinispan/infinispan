package org.infinispan.cli.commands;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CommandInputLine {
   private final String name;
   private final HashMap<String, String> options;
   private final HashMap<String, String> arguments;

   public CommandInputLine(String name) {
      this.name = name;
      this.options = new LinkedHashMap<>();
      this.arguments = new LinkedHashMap<>();
   }

   public CommandInputLine arg(String name, String value) {
      this.arguments.put(name, value);
      return this;
   }

   public CommandInputLine option(String name, boolean value) {
      this.options.put(name, Boolean.toString(value));
      return this;
   }

   public CommandInputLine optionalArg(String name, String value) {
      if (value != null) {
         this.arguments.put(name, value);
      }
      return this;
   }

   public String arg(String arg) {
      return arguments.get(arg);
   }

   public String option(String option) {
      return options.get(option);
   }

   public String toString() {
      StringBuilder sb = new StringBuilder(name);
      for (Map.Entry<String, String> option : options.entrySet()) {
         sb.append("--");
         sb.append(option.getKey());
         sb.append("=");
         sb.append(option.getValue());
      }
      for (String argument : arguments.values()) {
         sb.append(" ");
         sb.append(argument);
      }
      return sb.toString();
   }

   public String name() {
      return name;
   }

   public boolean hasArg(String arg) {
      return arguments.containsKey(arg);
   }
}
