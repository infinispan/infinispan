package org.infinispan.cli.interpreter.statement;

import java.util.List;
import org.infinispan.AdvancedCache;
import org.infinispan.cli.interpreter.codec.Codec;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.logging.LogFactory;

/**
 *
 * PutStatement puts an entry in the cache
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class PutStatement implements Statement {
   private static final Log log = LogFactory.getLog(PutStatement.class, Log.class);

   private enum Options {
      CODEC, IFABSENT
   };

   final KeyData keyData;
   final Object value;
   final Long expires;
   final Long maxIdle;
   final private List<Option> options;

   public PutStatement(final List<Option> options, final KeyData key, final Object value, final ExpirationData exp) {
      this.options = options;
      this.keyData = key;
      this.value = value;
      if (exp != null) {
         this.expires = exp.expires;
         this.maxIdle = exp.maxIdle;
      } else {
         this.expires = null;
         this.maxIdle = null;
      }
   }

   @Override
   public Result execute(Session session) throws StatementException {
      AdvancedCache<Object, Object> cache = session.getCache(keyData.getCacheName()).getAdvancedCache();
      Codec codec = session.getCodec();
      boolean overwrite = true;
      if (options.size() > 0) {
         for (Option option : options) {
            switch (option.toEnum(Options.class)) {
            case CODEC: {
               if (option.getParameter() == null) {
                  throw log.missingOptionParameter(option.getName());
               } else {
                  codec = session.getCodec(option.getParameter());
               }
               break;
            }
            case IFABSENT: {
               overwrite = false;
               break;
            }
            }
         }
      }
      Object encodedKey = codec.encodeKey(keyData.getKey());
      Object encodedValue = codec.encodeValue(value);
      Metadata metadata = codec.encodeMetadata(cache, expires, maxIdle);

      if (overwrite) {
         cache.put(encodedKey, encodedValue, metadata);
      } else {
         cache.putIfAbsent(encodedKey, encodedValue, metadata);
      }

      return EmptyResult.RESULT;
   }
}
