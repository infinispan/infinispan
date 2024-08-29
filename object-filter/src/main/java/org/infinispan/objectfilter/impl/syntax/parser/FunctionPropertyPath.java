package org.infinispan.objectfilter.impl.syntax.parser;

import java.util.List;
import java.util.Objects;

import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.ql.Function;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.jboss.logging.Logger;

/**
 * A function applied to a property path, used in SELECT or ORDER BY.
 *
 * @author anistor@redhat.com
 * @since 14.0
 */
public final class FunctionPropertyPath<TypeMetadata> extends PropertyPath<TypeDescriptor<TypeMetadata>> {

   private static final Log log = Logger.getMessageLogger(Log.class, FunctionPropertyPath.class.getName());

   private final Function function;

   private final List<Object> args;

   public FunctionPropertyPath(PropertyPath<TypeDescriptor<TypeMetadata>> path, Function function, List<Object> args) {
      this(path.getNodes(), function, args);
   }

   public FunctionPropertyPath(List<PropertyReference<TypeDescriptor<TypeMetadata>>> path, Function function, List<Object> args) {
      super(path);
      switch (function) {
         case DISTANCE:
            break;
         default:
            throw log.functionNotSupportedException(function.name());
      }
      this.function = function;
      this.args = args;
   }

   public Function getFunction() {
      return function;
   }

   public List<Object> getArgs() {
      return args;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      FunctionPropertyPath<?> that = (FunctionPropertyPath<?>) o;
      return function == that.function && Objects.equals(args, that.args);
   }

   @Override
   public int hashCode() {
      int hash = 31 * super.hashCode() + function.hashCode();
      if (args != null) {
         for (Object arg : args) {
            hash = 31 * hash + (arg != null ? arg.hashCode() : 0);
         }
      }
      return hash;
   }

   @Override
   public String toString() {
      return "FunctionPropertyPath{" +
            "function=" + function +
            ", propertyPath=" + asStringPath() +
            ", args=" + args +
            '}';
   }
}
