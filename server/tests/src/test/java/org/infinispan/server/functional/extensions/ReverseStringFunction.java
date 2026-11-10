package org.infinispan.server.functional.extensions;

import java.util.List;

import org.infinispan.api.query.functions.QueryFunction;
import org.infinispan.api.query.functions.QueryFunctionContext;
import org.infinispan.api.query.functions.QueryFunctionSignature;
import org.infinispan.filter.NamedFactory;

@NamedFactory(name = "reverse")
public class ReverseStringFunction implements QueryFunction {

   @QueryFunctionSignature(args = String.class, returns = String.class, category = "string")
   @Override
   public Object evaluate(List<Object> args, QueryFunctionContext ctx) {
      if (args.size() != 1) {
         throw new IllegalArgumentException("Function reverse expects 1 argument but got " + args.size());
      }
      Object s = args.get(0);
      return s == null ? null : new StringBuilder(String.valueOf(s)).reverse().toString();
   }
}
