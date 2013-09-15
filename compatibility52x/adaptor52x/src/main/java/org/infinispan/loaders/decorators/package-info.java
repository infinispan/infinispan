/**
 * This package contains loaders and stores, which are used for overflow or persistence.
 * Need @XmlSchema annotation for AsyncStoreConfig.java and SingletonStoreConfig.java
 */
@XmlSchema(namespace = ISPN_NS, elementFormDefault = XmlNsForm.QUALIFIED, attributeFormDefault = XmlNsForm.UNQUALIFIED, 
         xmlns = {
         @javax.xml.bind.annotation.XmlNs(prefix = "tns", namespaceURI = ISPN_NS),
         @javax.xml.bind.annotation.XmlNs(prefix = "xs", namespaceURI = "http://www.w3.org/2001/XMLSchema") })
package org.infinispan.loaders.decorators;

import javax.xml.bind.annotation.XmlNsForm;
import javax.xml.bind.annotation.*;
import static org.infinispan.config.parsing.NamespaceFilter.ISPN_NS;