/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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

import org.infinispan.config.AbstractConfigurationBean;
import org.infinispan.config.Configuration;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.w3c.dom.Element;

public class TransactionConfigReader implements ConfigurationElementReader {

   private AutomatedXmlConfigurationParserImpl parser;

   public TransactionConfigReader() {
      super();
   }

   public void setParser(AutomatedXmlConfigurationParserImpl parser) {
      this.parser = parser;
   }

   public void process(Element e, AbstractConfigurationBean bean) {
      if (e == null)
         return; // we might not have this configured

      Configuration config = (Configuration) bean;
      String tmp = parser.getAttributeValue(e, "transactionManagerLookupClass");
      if (parser.existsAttribute(tmp)) {
         config.setTransactionManagerLookupClass(tmp);
      } else {
         // use defaults since the transaction element is still present!
         config.setTransactionManagerLookupClass(GenericTransactionManagerLookup.class.getName());
      }
      parser.visitElementWithNoCustomReader(e, config);
   }
}
