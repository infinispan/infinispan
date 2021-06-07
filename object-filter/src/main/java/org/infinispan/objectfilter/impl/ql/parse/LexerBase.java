package org.infinispan.objectfilter.impl.ql.parse;

import java.io.PrintStream;

import org.antlr.runtime.CharStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.RecognizerSharedState;

/**
 * Base class for the generated lexer.
 *
 * @author anistor@redhat.com
 * @since 13.0
 */
public abstract class LexerBase extends Lexer {

   private PrintStream errStream;

   protected LexerBase() {
   }

   protected LexerBase(CharStream input, RecognizerSharedState state) {
      super(input, state);
   }

   public void setErrStream(PrintStream errStream) {
      this.errStream = errStream;
   }

   @Override
   public void emitErrorMessage(String msg) {
      if (errStream != null) {
         errStream.println(msg);
      }
   }
}
