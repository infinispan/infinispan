/**
 * This package contains loaders and stores, which are used for overflow or persistence.
 * Need @XmlSchema annotation for CacheLoaderConfig.java and AbstractCacheStoreConfig.java
 */
@XmlSchema(namespace = "urn:infinispan:config:4.0", elementFormDefault = XmlNsForm.QUALIFIED, attributeFormDefault = XmlNsForm.UNQUALIFIED, 
         xmlns = {
         @javax.xml.bind.annotation.XmlNs(prefix = "tns", namespaceURI = "urn:infinispan:config:4.0"),
         @javax.xml.bind.annotation.XmlNs(prefix = "xs", namespaceURI = "http://www.w3.org/2001/XMLSchema") })
package org.infinispan.loaders;

import javax.xml.bind.annotation.XmlNsForm;
import javax.xml.bind.annotation.*;