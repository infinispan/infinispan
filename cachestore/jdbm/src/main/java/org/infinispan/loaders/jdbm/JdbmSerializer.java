/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.jdbm;

import java.io.IOException;

import org.infinispan.marshall.StreamingMarshaller;

import jdbm.helper.Serializer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Uses the configured (runtime) {@link org.infinispan.marshall.StreamingMarshaller} of the cache.
 * This Serializer is thus not really serializable.
 * 
 * @author Elias Ross
 */
@SuppressWarnings("serial")
public class JdbmSerializer implements Serializer {
    private static final Log log = LogFactory.getLog(JdbmSerializer.class);
    
    private transient StreamingMarshaller marshaller;

    /**
     * Constructs a new JdbmSerializer.
     */
    public JdbmSerializer(StreamingMarshaller marshaller) {
        if (marshaller == null)
            throw new NullPointerException("marshaller");
        this.marshaller = marshaller;
    }

    @Override
    public Object deserialize(byte[] buf) throws IOException {
        try {
            return marshaller.objectFromByteBuffer(buf);
        } catch (ClassNotFoundException e) {
            throw (IOException)new IOException().initCause(e);
        }
    }

    @Override
    public byte[] serialize(Object obj) throws IOException {
       try {
          return marshaller.objectToByteBuffer(obj);
       } catch (InterruptedException e) {
          if (log.isTraceEnabled()) log.trace("Interrupted while serializing object"); 
          Thread.currentThread().interrupt();
          throw new IOException(e);
       }
    }

}
