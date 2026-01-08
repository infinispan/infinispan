package org.infinispan.cli.converters;

import org.infinispan.cli.printers.PrettyPrinter;

public class PrettyPrintConverter extends EnumConverter<PrettyPrinter.PrettyPrintMode> {
   public PrettyPrintConverter() {
      super(PrettyPrinter.PrettyPrintMode.class);
   }
}
