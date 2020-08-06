package org.infinispan.cli.commands;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class CommandInputLine {
   private final String name;
   private final HashMap<String, Object> options;
   private final HashMap<String, Object> arguments;

   public CommandInputLine(String name) {
      this.name = name;
      this.options = new LinkedHashMap<>();
      this.arguments = new LinkedHashMap<>();
   }

   public <T> CommandInputLine arg(String name, T value) {
      this.arguments.put(name, value);
      return this;
   }

   public CommandInputLine optionalArg(String name, Object value) {
      if (value != null) {
         this.arguments.put(name, value);
      }
      return this;
   }

   public CommandInputLine option(String name, String value) {
      if (value != null) {
         this.options.put(name, value);
      }
      return this;
   }

   public CommandInputLine option(String name, Integer value) {
      if (value != null) {
         this.options.put(name, value);
      }
      return this;
   }

   public CommandInputLine option(String name, Long value) {
      if (value != null) {
         this.options.put(name, value);
      }
      return this;
   }

   public CommandInputLine option(String name, boolean value) {
      this.options.put(name, Boolean.valueOf(value));
      return this;
   }

   public String arg(String arg) {
      return (String) arguments.get(arg);
   }

   public <T> T argAs(String arg) {
      return (T) arguments.get(arg);
   }

   public String option(String option) {
      return (String) options.get(option);
   }

   public Integer intOption(String option) {
      return (Integer) options.get(option);
   }

   public Long longOption(String option) {
      return (Long) options.get(option);
   }

   public Boolean boolOption(String option) {
      return (Boolean) options.get(option);
   }

   public boolean hasArg(String arg) {
      return arguments.containsKey(arg);
   }

   public boolean hasOption(String option) {
      return options.containsKey(option);
   }

   public <T> T optionAs(String arg) {
      return (T) options.get(arg);
   }

   public <T> T optionOrDefault(String option, Supplier<T> supplier) {
      T retVal = optionAs(option);
      return retVal == null ? supplier.get() : retVal;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder(name);
      for (Map.Entry<String, Object> option : options.entrySet()) {
         sb.append(" --");
         sb.append(option.getKey());
         sb.append("=");
         sb.append(option.getValue());
      }
      for (Object argument : arguments.values()) {
         sb.append(" ");
         sb.append(argument);
      }
      return sb.toString();
   }

   public String name() {
      return name;
   }
}
