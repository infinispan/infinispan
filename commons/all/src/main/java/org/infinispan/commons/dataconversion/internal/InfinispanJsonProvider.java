package org.infinispan.commons.dataconversion.internal;

import java.io.InputStream;
import java.nio.charset.Charset;

import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPathException;
import com.jayway.jsonpath.spi.json.AbstractJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;

public class InfinispanJsonProvider extends AbstractJsonProvider {

   @Override
   public Object parse(String json) throws InvalidJsonException {
      try {
         return Json.read(json);
      } catch (Exception e) {
         throw new InvalidJsonException(e.getCause());
      }
   }

   @Override
   public Object parse(InputStream jsonStream, String charset) throws InvalidJsonException {
      return Json.fetchContent(jsonStream, Charset.forName(charset));
   }

   @Override
   public String toJson(Object obj) {
      return Json.factory().make(obj).toString();
   }

   @Override
   public Object createArray() {
      return Json.array().asJsonList();
   }

   @Override
   public Object createMap() {
      return Json.object().asJsonMap();
   }

   @Override
   public boolean isMap(Object obj) {
      if (obj instanceof Json j) {
         return j.isObject();
      } return false;
      // return super.isMap(obj);
   }

   @Override
   public Object getMapValue(Object obj, String key) {
      // if (super.isMap(obj)) {
      //    return super.getMapValue(obj, key);
      // }
      if (isMap(obj)) {
         var map = ((Json)obj).asJsonMap();
         if (map.containsKey(key)) {
            return ((Json)map.get(key));
         }
      }
      return JsonProvider.UNDEFINED;
   }
       /**
     * Sets a value in an object
     *
     * @param obj   an object
     * @param key   a String key
     * @param value the value to set
     */
    public void setProperty(Object obj, Object key, Object value) {
        if (isMap(obj))
            ((Json.ObjectJson)obj).set(key.toString(), value);
        else {
            throw new JsonPathException("setProperty operation cannot be used with " + obj!=null?obj.getClass().getName():"null");
        }
    }


   @Override
   public boolean isArray(Object obj) {
      return (obj instanceof Json.ArrayJson) || super.isArray(obj);
   }

   /**
    * Get the length of an array or object
    *
    * @param obj an array or an object
    * @return the number of entries in the array or object
    */
   public int length(Object obj) {
      if (super.isArray(obj)) {
            return super.length(obj);
         }
      if (isArray(obj)) {
            return ((Json.ArrayJson)obj).asJsonList().size();
      }
      if (isMap(obj)) {
         return ((Json.ObjectJson)obj).asJsonMap().size();
      }
      if (obj instanceof String) {
         return ((String) obj).length();
      }
      throw new JsonPathException("length operation cannot be applied to " + (obj != null ? obj.getClass().getName()
            : "null"));
   }

       /**
     * Extracts a value from an array
     *
     * @param obj an array
     * @param idx index
     * @return the entry at the given index
     */
    public Object getArrayIndex(Object obj, int idx) {
      if (super.isArray(obj)) {
         return super.getArrayIndex(obj, idx);
      }
      if (isArray(obj)) {
         return ((Json.ArrayJson) obj).at(idx);
      }
      throw new UnsupportedOperationException();
  }

//   @SuppressWarnings("unchecked")
//   public void setArrayIndex(Object array, int index, Object newValue) {
//      if (array instanceof List) {
//         @SuppressWarnings("rawtypes")
//         List l = (List) array;
//         if (index == l.size()) {
//            l.add(newValue);
//         } else {
//            l.set(index, newValue);
//         }
//         return;
//      }
//      if (isArray(array)) {
//         var l = (Json.ArrayJson) array;
//         if (index == l.asJsonList().size()) {
//            l.add(newValue);
//         } else {
//            l.set(index, newValue);
//         }
//      }
//      throw new UnsupportedOperationException();
//   }
//       /**
//      * Sets a value in an object
//      *
//      * @param obj   an object
//      * @param key   a String key
//      * @param value the value to set
//      */
//     public void setProperty(Object obj, Object key, Object value) {
//         if (isMap(obj)) {
//             ((Json)obj).set(key.toString(), value);
//         }
//         else {
//             throw new JsonPathException("setProperty operation cannot be used with " + obj!=null?obj.getClass().getName():"null");
//         }
//       }

//       /**
//        * Returns the keys from the given object
//        *
//        * @param obj an object
//        * @return the keys for an object
//        */
//       @SuppressWarnings("unchecked")
//       public Collection<String> getPropertyKeys(Object obj) {
//          if (isArray(obj)) {
//             throw new UnsupportedOperationException();
//          } else {
//             if (super.isMap(obj)) {
//                return super.getPropertyKeys(obj);
//             }
//             return ((Json.ObjectJson) obj).asJsonMap().keySet();
//          }
//       }
   }
