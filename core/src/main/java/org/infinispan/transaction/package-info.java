/**
 * JTA transaction support.
 */
@XmlSchema(
      namespace =ISPN_NS,
      elementFormDefault = QUALIFIED,
      attributeFormDefault = UNQUALIFIED,
      xmlns = {
         @XmlNs(prefix = "tns", namespaceURI = ISPN_NS),
         @XmlNs(prefix = "xs", namespaceURI = "http://www.w3.org/2001/XMLSchema") }
)

package org.infinispan.transaction;

import javax.xml.bind.annotation.XmlNs;
import javax.xml.bind.annotation.XmlSchema;

import static javax.xml.bind.annotation.XmlNsForm.QUALIFIED;
import static javax.xml.bind.annotation.XmlNsForm.UNQUALIFIED;
import static org.infinispan.config.parsing.NamespaceFilter.ISPN_NS;