package org.infinispan.configuration.parsing;

import java.util.ArrayList;
import java.util.List;

public class AbstractParserContext implements ParserContext {
   List<ParserContextListener> listeners = new ArrayList<ParserContextListener>();

   @Override
   public void addParsingCompleteListener(ParserContextListener l) {
      listeners.add(l);
   }

   @Override
   public void fireParsingComplete() {
      for(ParserContextListener l : listeners) {
         l.parsingComplete(this);
      }
   }

}
