package org.infinispan.cli.interpreter.statement;

import java.util.Collection;
import java.util.List;

import org.infinispan.cli.interpreter.codec.Codec;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;

/**
 *
 * EncodingStatement selects a codec to use for encoding/decoding keys/values from the cli to the
 * cache and viceversa
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class EncodingStatement implements Statement {
   private enum Options {
      LIST
   };

   final String encoding;
   final private List<Option> options;

   public EncodingStatement(List<Option> options, String encoding) {
      this.encoding = encoding;
      this.options = options;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      for (Option option : options) {
         switch (option.toEnum(Options.class)) {
         case LIST: {
            StringBuilder sb = new StringBuilder();
            Collection<Codec> codecs = session.getCodecs();
            for (Codec codec : codecs) {
               sb.append(codec.getName());
               sb.append("\n");
            }
            return new StringResult(sb.toString());
         }
         }
      }
      if (encoding != null) {
         session.setCodec(encoding);
         return EmptyResult.RESULT;
      } else {
         return new StringResult(session.getCodec().getName());
      }
   }
}
