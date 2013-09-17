package org.infinispan.config;

import java.util.Set;
import java.util.Map.Entry;

import javax.xml.bind.annotation.adapters.XmlAdapter;

import org.infinispan.util.TypedProperties;
/**
 * TypedPropertiesAdapter is JAXB XmlAdapter for TypedProperties.
 *
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class TypedPropertiesAdapter extends XmlAdapter<PropertiesType, TypedProperties> {
   
   @Override
   public PropertiesType marshal(TypedProperties tp) throws Exception {
      PropertiesType pxml = new PropertiesType();
      Property[] pa = new Property[tp.size()];
      Set<Entry<Object, Object>> set = tp.entrySet();
      int index = 0;
      for (Entry<Object, Object> entry : set) {
         pa[index] = new Property();
         pa[index].name = (String) entry.getKey();
         pa[index].value = (String) entry.getValue();
         index++;
      }
      pxml.properties = pa;
      return pxml;
   }

   @Override
   public TypedProperties unmarshal(PropertiesType props) throws Exception {
      TypedProperties tp = new TypedProperties();
      if (props != null && props.properties != null) {
         for (Property p : props.properties) {
            tp.put(p.name, p.value);
         }
      }
      return tp;
   }
}
