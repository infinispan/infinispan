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
package org.infinispan.configuration.parsing;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ServiceLoader;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.infinispan.config.ConfigurationException;
import org.infinispan.util.FileLookup;
import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.Util;
import org.jboss.staxmapper.XMLMapper;

/**
 *
 * ParserRegistry.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ParserRegistry {
   private final XMLMapper xmlMapper;
   private final ClassLoader cl;

   public ParserRegistry(ClassLoader classLoader) {
      xmlMapper = XMLMapper.Factory.create();
      this.cl = classLoader;
      ServiceLoader<ConfigurationParser> parsers = ServiceLoader.load(ConfigurationParser.class, cl);
      for (ConfigurationParser<?> parser : parsers) {
         for (Namespace ns : parser.getSupportedNamespaces()) {
            xmlMapper.registerRootElement(new QName(ns.getUri(), ns.getRootElement()), parser);
         }
      }
   }

   public ConfigurationBuilderHolder parseFile(String filename) throws IOException {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      InputStream is = fileLookup.lookupFile(filename, cl);
      if(is==null) {
         throw new FileNotFoundException(filename);
      }
      try {
         return parse(is);
      } finally {
         Util.close(is);
      }
   }

   public ConfigurationBuilderHolder parse(InputStream is) {
      try {

         BufferedInputStream input = new BufferedInputStream(is);
         XMLStreamReader streamReader = XMLInputFactory.newInstance().createXMLStreamReader(input);
         ConfigurationBuilderHolder holder = new ConfigurationBuilderHolder(cl);
         xmlMapper.parseDocument(holder, streamReader);
         streamReader.close();
         return holder;
      } catch (ConfigurationException e) {
         throw e;
      } catch (Exception e) {
         throw new ConfigurationException(e);
      }
   }
}
