package org.infinispan.cli.interpreter;

import java.util.List;

import org.infinispan.commons.util.Util;

public class ParseException extends Exception {

   public ParseException(List<String> parserErrors) {
      super(Util.join(parserErrors, System.getProperty("line.separator")));
   }

}
