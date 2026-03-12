package org.infinispan.cli.converters;

import org.infinispan.cli.completers.ExposeCompleter;

public class ExposeConverter extends EnumConverter<ExposeCompleter.Expose> {
   public ExposeConverter() {
      super(ExposeCompleter.Expose.class);
   }
}
