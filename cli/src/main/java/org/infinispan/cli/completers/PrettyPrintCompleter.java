package org.infinispan.cli.completers;

import org.infinispan.cli.printers.PrettyPrinter;

public class PrettyPrintCompleter extends EnumCompleter<PrettyPrinter.PrettyPrintMode> {

   public PrettyPrintCompleter() {
      super(PrettyPrinter.PrettyPrintMode.class);
   }
}
