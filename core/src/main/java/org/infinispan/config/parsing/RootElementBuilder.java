/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.config.parsing;

import org.infinispan.config.ConfigurationException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.util.xml.JBossEntityResolver;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;

/**
 * Parses an xml files and validates xml elements form {@link RootElementBuilder#HORIZON_NS} namespace according to the
 * configured schema.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class RootElementBuilder {

   private static final JBossEntityResolver resolver = new JBossEntityResolver();

   public static final String HORIZON_NS = "urn:infinispan:config:4.0";

   static {
      // Globally register this namespace
      JBossEntityResolver.registerEntity(HORIZON_NS, "infinispan-config-4.0.xsd");
   }

   private static final Log log = LogFactory.getLog(RootElementBuilder.class);
   private ErrorHandler errorHandler;
   private boolean isValidating;
   public static final String VALIDATING_SYSTEM_PROPERTY = "infinispan.config.validate";

   public RootElementBuilder(ErrorHandler errorHandler) {
      this.errorHandler = errorHandler;
      isValidating = System.getProperty(VALIDATING_SYSTEM_PROPERTY) == null || Boolean.getBoolean(VALIDATING_SYSTEM_PROPERTY);
   }

   public RootElementBuilder(ErrorHandler errorHandler, boolean validating) {
      this.errorHandler = errorHandler;
      isValidating = validating;
   }

   public RootElementBuilder() {
      this(new FailureErrorHandler());
   }

   public RootElementBuilder(boolean validating) {
      this(new FailureErrorHandler(), validating);
   }

   public Element readRoot(InputStream config) {
      try {
         DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
         docBuilderFactory.setNamespaceAware(true);
         if (isValidating) {
            docBuilderFactory.setValidating(true);
            docBuilderFactory.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaLanguage", "http://www.w3.org/2001/XMLSchema");
            String[] value = {HORIZON_NS};
            docBuilderFactory.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaSource", value);
         }
         DocumentBuilder parser = docBuilderFactory.newDocumentBuilder();
         parser.setEntityResolver(resolver);
         parser.setErrorHandler(errorHandler);
         Document doc = parser.parse(config);
         Element root = doc.getDocumentElement();
         root.normalize();
         return root;
      }
      catch (Exception e) {
         log.error(e);
         throw new ConfigurationException("Could not parse the config file", e);
      }
   }

   /**
    * Default schema validation error handler, that throws an exception on validation errors.
    */
   private static class FailureErrorHandler implements ErrorHandler {
      public void warning(SAXParseException exception) throws SAXException {
         logAndThrowException(exception);
      }

      public void error(SAXParseException exception) throws SAXException {
         logAndThrowException(exception);
      }

      public void fatalError(SAXParseException exception) throws SAXException {
         logAndThrowException(exception);
      }

      private void logAndThrowException(SAXParseException exception) {
         log.error("Configuration warning: " + exception.getMessage());
         throw new ConfigurationException("Incorrect configuration file. Use '-Dhorizon.config.validate=false' to disable validation.", exception);
      }
   }

   public boolean isValidating() {
      return isValidating;
   }
}
