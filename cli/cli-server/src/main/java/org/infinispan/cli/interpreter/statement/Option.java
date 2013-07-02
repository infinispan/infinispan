package org.infinispan.cli.interpreter.statement;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.util.logging.LogFactory;

/**
 * CLI Command option
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class Option {
   private static final Log log = LogFactory.getLog(Option.class, Log.class);
   final String name;
   final String parameter;

   public Option(String name) {
      this(name, null);
   }

   public Option(String name, String parameter) {
      this.name = name;
      this.parameter = parameter;
   }

   public String getName() {
      return name;
   }

   public String getParameter() {
      return parameter;
   }

   @Override
   public String toString() {
      return name;
   }

   public <T extends Enum<T>> T toEnum(Class<T> enumType) throws StatementException {
      try {
         return Enum.valueOf(enumType, name.toUpperCase());
      } catch (IllegalArgumentException e) {
         throw log.unknownOption(name);
      }
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((name == null) ? 0 : name.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      Option other = (Option) obj;
      if (name == null) {
         if (other.name != null)
            return false;
      } else if (!name.equals(other.name))
         return false;
      return true;
   }

}
