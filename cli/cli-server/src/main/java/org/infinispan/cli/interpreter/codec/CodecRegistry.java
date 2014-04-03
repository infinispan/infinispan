package org.infinispan.cli.interpreter.codec;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceConfigurationError;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.util.logging.LogFactory;

/**
 * CodecRegistry.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class CodecRegistry {
   public static final Log log = LogFactory.getLog(CodecRegistry.class, Log.class);
   private Map<String, Codec> codecs;

   public CodecRegistry(ClassLoader cl) {
      codecs = new HashMap<String, Codec>();
      Iterator<Codec> it = ServiceFinder.load(Codec.class, cl).iterator();
      for(;;) {
         try {
            Codec codec = it.next();
            String name = codec.getName();
            if (codecs.containsKey(name)) {
               log.duplicateCodec(codec.getClass().getName(), codecs.get(name).getClass().getName());
            } else {
               codecs.put(name, codec);
            }
         } catch (ServiceConfigurationError e) {
            log.loadingCodecFailed(e);
         } catch (NoSuchElementException e) {
            break;
         }
      }
   }

   public Collection<Codec> getCodecs() {
      return codecs.values();
   }

   public Codec getCodec(String name) {
      return codecs.get(name);
   }
}
