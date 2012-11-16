/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.interpreter.codec;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import org.infinispan.cli.interpreter.logging.Log;
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
      ServiceLoader<Codec> services = ServiceLoader.load(Codec.class, cl);
      Iterator<Codec> it = services.iterator();
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
