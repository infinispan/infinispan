package org.infinispan.config.parsing;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 * Adds namespace where needed when parsing an XML document
 */
public class NamespaceFilter extends XMLFilterImpl {

   private static final String NAMESPACE = "urn:infinispan:config:4.0";

   //State variable
   private boolean addedNamespace = false;

   @Override
   public void startDocument() throws SAXException {
      super.startDocument();
      startControlledPrefixMapping();
   }


   @Override
   public void startElement(String arg0, String arg1, String arg2,
                            Attributes arg3) throws SAXException {

      super.startElement(this.NAMESPACE, arg1, arg2, arg3);
   }

   @Override
   public void endElement(String arg0, String arg1, String arg2)
           throws SAXException {

      super.endElement(this.NAMESPACE, arg1, arg2);
   }

   @Override
   public void startPrefixMapping(String prefix, String url)
           throws SAXException {
      this.startControlledPrefixMapping();
   }

   private void startControlledPrefixMapping() throws SAXException {

      if (!this.addedNamespace) {
         //We should add namespace since it is set and has not yet been done.
         super.startPrefixMapping("", this.NAMESPACE);

         //Make sure we dont do it twice
         this.addedNamespace = true;
      }
   }

}