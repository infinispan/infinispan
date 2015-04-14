package org.infinispan.cli.interpreter.statement;

import java.util.List;

import org.infinispan.cli.interpreter.codec.Codec;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.util.logging.LogFactory;

public abstract class CodecAwareStatement implements Statement {
   private static final Log log = LogFactory.getLog(CodecAwareStatement.class, Log.class);

   private enum Options {
      CODEC
   };

   final private List<Option> options;

   CodecAwareStatement(List<Option> options) {
      this.options = options;
   }

   Codec getCodec(Session session) throws StatementException {
      if (options.size() > 0) {
         for (Option option : options) {
            switch (option.toEnum(Options.class)) {
            case CODEC: {
               if (option.getParameter() == null) {
                  throw log.missingOptionParameter(option.getName());
               } else {
                  return session.getCodec(option.getParameter());
               }
            }
            default:
               break;
            }
         }
      }
      return session.getCodec();
   }

}
