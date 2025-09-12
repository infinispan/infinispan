package org.infinispan.commons.dataconversion.internal;

/*
 * Copyright (C) 2011 Miami-Dade County.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Note: this file incorporates source code from 3d party entities. Such code
 * is copyrighted by those entities as indicated below.
 */

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.TimeQuantity;

/**
 * <p>
 * Represents a JSON (JavaScript Object Notation) entity. For more information about JSON, please see
 * <a href="http://www.json.org" target="_">http://www.json.org</a>.
 * </p>
 *
 * <p>
 * A JSON entity can be one of several things: an object (set of name/Json entity pairs), an array (a list of other JSON
 * entities), a string, a number, a boolean or null. All of those are represented as <code>Json</code> instances. Each
 * of the different types of entities supports a different set of operations. However, this class unifies all operations
 * into a single interface so in Java one is always dealing with a single object type: this class. The approach
 * effectively amounts to dynamic typing where using an unsupported operation won't be detected at compile time, but
 * will throw a runtime {@link UnsupportedOperationException}. It simplifies working with JSON structures considerably
 * and it leads to shorter at cleaner Java code. It makes much easier to work with JSON structure without the need to
 * convert to "proper" Java representation in the form of POJOs and the like. When traversing a JSON, there's no need to
 * type-cast at each step because there's only one type: <code>Json</code>.
 * </p>
 *
 * <p>
 * One can examine the concrete type of a <code>Json</code> with one of the <code>isXXX</code> methods:
 * {@link #isObject()}, {@link #isArray()},{@link #isNumber()},{@link #isBoolean()},{@link #isString()},
 * {@link #isNull()}.
 * </p>
 *
 * <p>
 * The underlying representation of a given <code>Json</code> instance can be obtained by calling the generic
 * {@link #getValue()} method or one of the <code>asXXX</code> methods such as {@link #asBoolean()} or
 * {@link #asString()} etc. JSON objects are represented as Java {@link Map}s while JSON arrays are represented as Java
 * {@link List}s. Because those are mutable aggregate structures, there are two versions of the corresponding
 * <code>asXXX</code> methods: {@link #asMap()} which performs a deep copy of the underlying map, unwrapping every
 * nested Json entity to its Java representation and {@link #asJsonMap()} which simply return the map reference.
 * Similarly, there are {@link #asList()} and {@link #asJsonList()}.
 * </p>
 *
 * <h2>Constructing and Modifying JSON Structures</h2>
 *
 * <p>
 * There are several static factory methods in this class that allow you to create new
 * <code>Json</code> instances:
 * </p>
 *
 * <table>
 * <caption>Static factory methods</caption>
 * <tr><td>{@link #read(String)}</td>
 * <td>Parse a JSON string and return the resulting <code>Json</code> instance. The syntax
 * recognized is as defined in <a href="http://www.json.org">http://www.json.org</a>.
 * </td>
 * </tr>
 * <tr><td>{@link #make(Object)}</td>
 * <td>Creates a Json instance based on the concrete type of the parameter. The types
 * recognized are null, numbers, primitives, String, Map, Collection, Java arrays
 * and <code>Json</code> itself.</td>
 * </tr>
 * <tr><td>{@link #nil()}</td>
 * <td>Return a <code>Json</code> instance representing JSON <code>null</code>.</td>
 * </tr>
 * <tr><td>{@link #object()}</td>
 * <td>Create and return an empty JSON object.</td>
 * </tr>
 * <tr><td>{@link #object(Object...)}</td>
 * <td>Create and return a JSON object populated with the key/value pairs
 * passed as an argument sequence. Each even parameter becomes a key (via
 * <code>toString</code>) and each odd parameter is converted to a <code>Json</code>
 * value.</td>
 * </tr>
 * <tr><td>{@link #array()}</td>
 * <td>Create and return an empty JSON array.</td>
 * </tr>
 * <tr><td>{@link #array(Object...)}</td>
 * <td>Create and return a JSON array from the list of arguments.</td>
 * </tr>
 * </table>
 *
 * <p>
 * To customize how Json elements are represented and to provide your own version of the
 * {@link #make(Object)} method, you create an implementation of the {@link Factory} interface
 * and configure it either globally with the {@link #setGlobalFactory(Factory)} method or
 * on a per-thread basis with the {@link #attachFactory(Factory)}/{@link #detachFactory()}
 * methods.
 * </p>
 *
 * <p>
 * If a <code>Json</code> instance is an object, you can set its properties by
 * calling the {@link #set(String, Object)} method which will add a new property or replace an existing one.
 * Adding elements to an array <code>Json</code> is done with the {@link #add(Object)} method.
 * Removing elements by their index (or key) is done with the {@link #delAt(int)} (or
 * {@link #delAt(String)}) method. You can also remove an element from an array without
 * knowing its index with the {@link #remove(Object)} method. All these methods return the
 * <code>Json</code> instance being manipulated so that method calls can be chained.
 * If you want to remove an element from an object or array and return the removed element
 * as a result of the operation, call {@link #atDel(int)} or {@link #atDel(String)} instead.
 * </p>
 *
 * <p>
 * If you want to add properties to an object in bulk or append a sequence of elements to array,
 * use the {@link #with(Json, Json...opts)} method. When used on an object, this method expects another
 * object as its argument, and it will copy all properties of that argument into itself. Similarly,
 * when called on array, the method expects another array and it will append all elements of its
 * argument to itself.
 * </p>
 *
 * <p>
 * To make a clone of a Json object, use the {@link #dup()} method. This method will create a new
 * object even for the immutable primitive Json types. Objects and arrays are cloned
 * (i.e. duplicated) recursively.
 * </p>
 *
 * <h2>Navigating JSON Structures</h2>
 *
 * <p>
 * The {@link #at(int)} method returns the array element at the specified index and the
 * {@link #at(String)} method does the same for a property of an object instance. You can
 * use the {@link #at(String, Object)} version to create an object property with a default
 * value if it doesn't exist already.
 * </p>
 *
 * <p>
 * To test just whether a Json object has a given property, use the {@link #has(String)} method. To test
 * whether a given object property or an array elements is equal to a particular value, use the
 * {@link #is(String, Object)} and {@link #is(int, Object)} methods respectively. Those methods return
 * true if the given named property (or indexed element) is equal to the passed in Object as the second
 * parameter. They return false if an object doesn't have the specified property or an index array is out
 * of bounds. For example is(name, value) is equivalent to 'has(name) &amp;&amp; at(name).equals(make(value))'.
 * </p>
 *
 * <p>
 * To help in navigating JSON structures, instances of this class contain a reference to the
 * enclosing JSON entity (object or array) if any. The enclosing entity can be accessed
 * with {@link #up()} method.
 * </p>
 *
 * <p>
 * The combination of method chaining when modifying <code>Json</code> instances and
 * the ability to navigate "inside" a structure and then go back to the enclosing
 * element lets one accomplish a lot in a single Java statement, without the need
 * of intermediary variables. Here for example how the following JSON structure can
 * be created in one statement using chained calls:
 * </p>
 *
 * <pre><code>
 * {"menu": {
 * "id": "file",
 * "value": "File",
 * "popup": {
 *   "menuitem": [
 *     {"value": "New", "onclick": "CreateNewDoc()"},
 *     {"value": "Open", "onclick": "OpenDoc()"},
 *     {"value": "Close", "onclick": "CloseDoc()"}
 *   ]
 * }
 * "position": 0
 * }}
 * </code></pre>
 *
 * <pre><code>
 * import mjson.Json;
 * import static mjson.Json.*;
 * ...
 * Json j = object()
 *  .at("menu", object())
 *    .set("id", "file")
 *    .set("value", "File")
 *    .at("popup", object())
 *      .at("menuitem", array())
 *        .add(object("value", "New", "onclick", "CreateNewDoc()"))
 *        .add(object("value", "Open", "onclick", "OpenDoc()"))
 *        .add(object("value", "Close", "onclick", "CloseDoc()"))
 *        .up()
 *      .up()
 *    .set("position", 0)
 *  .up();
 * ...
 * </code></pre>
 *
 * <p>
 * If there's no danger of naming conflicts, a static import of the factory methods (<code>
 * import static json.Json.*;</code>) would reduce typing even further and make the code more
 * readable.
 * </p>
 *
 * <h2>Converting to String</h2>
 *
 * <p>
 * To get a compact string representation, simply use the {@link #toString()} method. If you
 * want to wrap it in a JavaScript callback (for JSON with padding), use the {@link #pad(String)}
 * method.
 * </p>
 *
 * <h2>Validating with JSON Schema</h2>
 *
 * <p>
 * Since version 1.3, mJson supports JSON Schema, draft 4. A schema is represented by the internal
 * class {@link Json.Schema}. To perform a validation, you have a instantiate a <code>Json.Schema</code>
 * using the factory method {@link Json.Schema} and then call its <code>validate</code> method
 * on a JSON instance:
 * </p>
 *
 * <pre><code>
 * import mjson.Json;
 * import static mjson.Json.*;
 * ...
 * Json inputJson = Json.read(inputString);
 * Json schema = Json.schema(new URI("http://mycompany.com/schemas/model"));
 * Json errors = schema.validate(inputJson);
 * for (Json error : errors.asJsonList())
 * System.out.println("Validation error " + err);
 * </code></pre>
 * <h2>
 * Infinispan changes on top of 1.4.2:
 * </h2>
 * <ul>
 * <li>Added support for pretty printing {@link Json#toPrettyString()}</li>
 * <li>Added support for {@link RawJson} as a specialized {@link StringJson}</li>
 * <li>Usage of {@link LinkedHashMap} internally for {@link ObjectJson} for predictable iteration</li>
 * <li>Support for {@link Class}, {@link Properties}, {@link Enum} for {@link DefaultFactory#make(Object)}</li>
 * <li>Support from internal Infinispan classes for {@link DefaultFactory#make(Object)}: {@link MediaType}, {@link JsonSerialization}</li>
 * <li>Support for replacing objects</li>
 * </ul>
 *
 * @author Borislav Iordanov
 * @version 1.4.2
 */
public class Json implements java.io.Serializable {
   private static final long serialVersionUID = 1L;

   /**
    * <p>
    * This interface defines how <code>Json</code> instances are constructed. There is a default implementation for each
    * kind of <code>Json</code> value, but you can provide your own implementation. For example, you might want a
    * different representation of an object than a regular <code>HashMap</code>. Or you might want string comparison to
    * be case insensitive.
    * </p>
    *
    * <p>
    * In addition, the {@link #make(Object)} method allows you plug-in your own mapping of arbitrary Java objects to
    * <code>Json</code> instances. You might want to implement a Java Beans to JSON mapping or any other JSON
    * serialization that makes sense in your project.
    * </p>
    *
    * <p>
    * To avoid implementing all methods in that interface, you can extend the {@link DefaultFactory} default
    * implementation and simply overwrite the ones you're interested in.
    * </p>
    *
    * <p>
    * The factory implementation used by the <code>Json</code> classes is specified simply by calling the
    * {@link #setGlobalFactory(Factory)} method. The factory is a static, global variable by default. If you need
    * different factories in different areas of a single application, you may attach them to different threads of
    * execution using the {@link #attachFactory(Factory)}. Recall a separate copy of static variables is made per
    * ClassLoader, so for example in a web application context, that global factory can be different for each web
    * application (as Java web servers usually use a separate class loader per application). Thread-local factories are
    * really a provision for special cases.
    * </p>
    *
    * @author Borislav Iordanov
    */
   public interface Factory {
      /**
       * Construct and return an object representing JSON <code>null</code>. Implementations are free to cache a return
       * the same instance. The resulting value must return
       * <code>true</code> from <code>isNull()</code> and <code>null</code> from
       * <code>getValue()</code>.
       *
       * @return The representation of a JSON <code>null</code> value.
       */
      Json nil();

      /**
       * Construct and return a JSON boolean. The resulting value must return
       * <code>true</code> from <code>isBoolean()</code> and the passed
       * in parameter from <code>getValue()</code>.
       *
       * @param value The boolean value.
       * @return A JSON with <code>isBoolean() == true</code>. Implementations are free to cache and return the same
       * instance for true and false.
       */
      Json bool(boolean value);

      /**
       * Construct and return a JSON string. The resulting value must return
       * <code>true</code> from <code>isString()</code> and the passed
       * in parameter from <code>getValue()</code>.
       *
       * @param value The string to wrap as a JSON value.
       * @return A JSON element with the given string as a value.
       */
      Json string(String value);

      Json raw(String value);

      /**
       * Construct and return a JSON number. The resulting value must return
       * <code>true</code> from <code>isNumber()</code> and the passed
       * in parameter from <code>getValue()</code>.
       *
       * @param value The numeric value.
       * @return Json instance representing that value.
       */
      Json number(Number value);

      /**
       * Construct and return a JSON object. The resulting value must return
       * <code>true</code> from <code>isObject()</code> and an implementation
       * of <code>java.util.Map</code> from <code>getValue()</code>.
       *
       * @return An empty JSON object.
       */
      Json object();

      /**
       * Construct and return a JSON object. The resulting value must return
       * <code>true</code> from <code>isArray()</code> and an implementation
       * of <code>java.util.List</code> from <code>getValue()</code>.
       *
       * @return An empty JSON array.
       */
      Json array();

      /**
       * Construct and return a JSON object. The resulting value can be of any JSON type. The method is responsible for
       * examining the type of its argument and performing an appropriate mapping to a <code>Json</code> instance.
       *
       * @param anything An arbitray Java object from which to construct a <code>Json</code> element.
       * @return The newly constructed <code>Json</code> instance.
       */
      Json make(Object anything);
   }

   public interface Function<T, R> {

      /**
       * Applies this function to the given argument.
       *
       * @param t the function argument
       * @return the function result
       */
      R apply(T t);
   }

   /**
    * <p>
    * Represents JSON schema - a specific data format that a JSON entity must follow. The idea of a JSON schema is very
    * similar to XML. Its main purpose is validating input.
    * </p>
    *
    * <p>
    * More information about the various JSON schema specifications can be found at http://json-schema.org. JSON Schema
    * is an  IETF draft (v4 currently) and our implementation follows this set of specifications. A JSON schema is
    * specified as a JSON object that contains keywords defined by the specification. Here are a few introductory
    * materials:
    * <ul>
    * <li>http://jsonary.com/documentation/json-schema/ -
    * a very well-written tutorial covering the whole standard</li>
    * <li>http://spacetelescope.github.io/understanding-json-schema/ -
    * online book, tutorial (Python/Ruby based)</li>
    * </ul>
    *
    * @author Borislav Iordanov
    */
   public interface Schema {
      /**
       * <p>
       * Validate a JSON document according to this schema. The validations attempts to proceed even in the face of
       * errors. The return value is always a <code>Json.object</code> containing the boolean property <code>ok</code>.
       * When <code>ok</code> is <code>true</code>, the return object contains nothing else. When it is
       * <code>false</code>, the return object contains a property <code>errors</code> which is an array of error
       * messages for all detected schema violations.
       * </p>
       *
       * @param document The input document.
       * @return <code>{"ok":true}</code> or <code>{"ok":false, errors:["msg1", "msg2", ...]}</code>
       */
      Json validate(Json document);

      /**
       * <p>Return the JSON representation of the schema.</p>
       */
      Json toJson();

      /**
       * <p>Possible options are: <code>ignoreDefaults:true|false</code>.
       * </p>
       * @return A newly created <code>Json</code> conforming to this schema.
       */
      //Json generate(Json options);
   }

   static String fetchContent(URL url) {
      java.io.Reader reader = null;
      try {
         reader = new java.io.InputStreamReader((java.io.InputStream) url.getContent(), StandardCharsets.UTF_8);
         StringBuilder content = new StringBuilder();
         char[] buf = new char[1024];
         for (int n = reader.read(buf); n > -1; n = reader.read(buf))
            content.append(buf, 0, n);
         return content.toString();
      } catch (Exception ex) {
         throw new RuntimeException(ex);
      } finally {
         if (reader != null) try {
            reader.close();
         } catch (Throwable t) {
         }
      }
   }

   static Json resolvePointer(String pointerRepresentation, Json top) {
      String[] parts = pointerRepresentation.split("/");
      Json result = top;
      for (String p : parts) {
         // TODO: unescaping and decoding
         if (p.isEmpty())
            continue;
         p = p.replace("~1", "/").replace("~0", "~");
         if (result.isArray())
            result = result.at(Integer.parseInt(p));
         else if (result.isObject())
            result = result.at(p);
         else
            throw new RuntimeException("Can't resolve pointer " + pointerRepresentation +
                  " on document " + top.toString(200));
      }
      return result;
   }

   static URI makeAbsolute(URI base, String ref) throws Exception {
      URI refuri;
      if (base != null && base.getAuthority() != null && !new URI(ref).isAbsolute()) {
         StringBuilder sb = new StringBuilder();
         if (base.getScheme() != null)
            sb.append(base.getScheme()).append("://");
         sb.append(base.getAuthority());
         if (!ref.startsWith("/")) {
            if (ref.startsWith("#"))
               sb.append(base.getPath());
            else {
               int slashIdx = base.getPath().lastIndexOf('/');
               sb.append(slashIdx == -1 ? base.getPath() : base.getPath().substring(0, slashIdx)).append("/");
            }
         }
         refuri = new URI(sb.append(ref).toString());
      } else if (base != null)
         refuri = base.resolve(ref);
      else
         refuri = new URI(ref);
      return refuri;
   }

   static Json resolveRef(URI base,
                          Json refdoc,
                          URI refuri,
                          Map<String, Json> resolved,
                          Map<Json, Json> expanded,
                          Function<URI, Json> uriResolver) throws Exception {
      if (refuri.isAbsolute() &&
            (base == null || !base.isAbsolute() ||
                  !base.getScheme().equals(refuri.getScheme()) ||
                  !Objects.equals(base.getHost(), refuri.getHost()) ||
                  base.getPort() != refuri.getPort() ||
                  !base.getPath().equals(refuri.getPath()))) {
         URI docuri = null;
         refuri = refuri.normalize();
         if (refuri.getHost() == null)
            docuri = new URI(refuri.getScheme() + ":" + refuri.getPath());
         else
            docuri = new URI(refuri.getScheme() + "://" + refuri.getHost() +
                  ((refuri.getPort() > -1) ? ":" + refuri.getPort() : "") +
                  refuri.getPath());
         refdoc = uriResolver.apply(docuri);
         refdoc = expandReferences(refdoc, refdoc, docuri, resolved, expanded, uriResolver);
      }
      if (refuri.getFragment() == null)
         return refdoc;
      else
         return resolvePointer(refuri.getFragment(), refdoc);
   }

   /**
    * <p>
    * Replace all JSON references, as per the http://tools.ietf.org/html/draft-pbryan-zyp-json-ref-03 specification, by
    * their referants.
    * </p>
    *
    * @param json
    * @param duplicate
    * @param done
    * @return
    */
   static Json expandReferences(Json json,
                                Json topdoc,
                                URI base,
                                Map<String, Json> resolved,
                                Map<Json, Json> expanded,
                                Function<URI, Json> uriResolver) throws Exception {
      if (expanded.containsKey(json)) return json;
      if (json.isObject()) {
         if (json.has("id") && json.at("id").isString()) // change scope of nest references
         {
            base = base.resolve(json.at("id").asString());
         }

         if (json.has("$ref")) {
            URI refuri = makeAbsolute(base, json.at("$ref").asString()); // base.resolve(json.at("$ref").asString());
            Json ref = resolved.get(refuri.toString());
            if (ref == null) {
               ref = Json.object();
               resolved.put(refuri.toString(), ref);
               ref.with(resolveRef(base, topdoc, refuri, resolved, expanded, uriResolver));
            }
            json = ref;
         } else {
            for (Map.Entry<String, Json> e : json.asJsonMap().entrySet())
               json.set(e.getKey(), expandReferences(e.getValue(), topdoc, base, resolved, expanded, uriResolver));
         }
      } else if (json.isArray()) {
         for (int i = 0; i < json.asJsonList().size(); i++)
            json.set(i,
                  expandReferences(json.at(i), topdoc, base, resolved, expanded, uriResolver));
      }
      expanded.put(json, json);
      return json;
   }

   static class DefaultSchema implements Schema {
      interface Instruction extends Function<Json, Json> {
      }

      static Json maybeError(Json errors, Json E) {
         return E == null ? errors : (errors == null ? Json.array() : errors).with(E, new Json[0]);
      }

      // Anything is valid schema
      static Instruction any = param -> null;

      // Type validation
      class IsObject implements Instruction {
         public Json apply(Json param) {
            return param.isObject() ? null : Json.make(param.toString(maxchars));
         }
      }

      class IsArray implements Instruction {
         public Json apply(Json param) {
            return param.isArray() ? null : Json.make(param.toString(maxchars));
         }
      }

      class IsString implements Instruction {
         public Json apply(Json param) {
            return param.isString() ? null : Json.make(param.toString(maxchars));
         }
      }

      class IsBoolean implements Instruction {
         public Json apply(Json param) {
            return param.isBoolean() ? null : Json.make(param.toString(maxchars));
         }
      }

      class IsNull implements Instruction {
         public Json apply(Json param) {
            return param.isNull() ? null : Json.make(param.toString(maxchars));
         }
      }

      class IsNumber implements Instruction {
         public Json apply(Json param) {
            return param.isNumber() ? null : Json.make(param.toString(maxchars));
         }
      }

      class IsInteger implements Instruction {
         public Json apply(Json param) {
            return param.isNumber() && ((Number) param.getValue()) instanceof Integer ? null : Json.make(param.toString(maxchars));
         }
      }

      class CheckString implements Instruction {
         int min = 0, max = Integer.MAX_VALUE;
         Pattern pattern;

         public Json apply(Json param) {
            Json errors = null;
            if (!param.isString()) return errors;
            String s = param.asString();
            final int size = s.codePointCount(0, s.length());
            if (size < min || size > max)
               errors = maybeError(errors, Json.make("String  " + param.toString(maxchars) +
                     " has length outside of the permitted range [" + min + "," + max + "]."));
            if (pattern != null && !pattern.matcher(s).matches())
               errors = maybeError(errors, Json.make("String  " + param.toString(maxchars) +
                     " does not match regex " + pattern.toString()));
            return errors;
         }
      }

      static class CheckNumber implements Instruction {
         double min = Double.NaN, max = Double.NaN, multipleOf = Double.NaN;
         boolean exclusiveMin = false, exclusiveMax = false;

         public Json apply(Json param) {
            Json errors = null;
            if (!param.isNumber()) return errors;
            double value = param.asDouble();
            if (!Double.isNaN(min) && (value < min || exclusiveMin && value == min))
               errors = maybeError(errors, Json.make("Number " + param + " is below allowed minimum " + min));
            if (!Double.isNaN(max) && (value > max || exclusiveMax && value == max))
               errors = maybeError(errors, Json.make("Number " + param + " is above allowed maximum " + max));
            if (!Double.isNaN(multipleOf) && (value / multipleOf) % 1 != 0)
               errors = maybeError(errors, Json.make("Number " + param + " is not a multiple of  " + multipleOf));
            return errors;
         }
      }

      class CheckArray implements Instruction {
         int min = 0, max = Integer.MAX_VALUE;
         Boolean uniqueitems = null;
         Instruction additionalSchema = any;
         Instruction schema;
         ArrayList<Instruction> schemas;

         public Json apply(Json param) {
            Json errors = null;
            if (!param.isArray()) return errors;
            if (schema == null && schemas == null && additionalSchema == null) // no schema specified
               return errors;
            int size = param.asJsonList().size();
            for (int i = 0; i < size; i++) {
               Instruction S = schema != null ? schema
                     : (schemas != null && i < schemas.size()) ? schemas.get(i) : additionalSchema;
               if (S == null)
                  errors = maybeError(errors, Json.make("Additional items are not permitted: " +
                        param.at(i) + " in " + param.toString(maxchars)));
               else
                  errors = maybeError(errors, S.apply(param.at(i)));
               if (uniqueitems != null && uniqueitems && param.asJsonList().lastIndexOf(param.at(i)) > i)
                  errors = maybeError(errors, Json.make("Element " + param.at(i) + " is duplicate in array."));
               if (errors != null && !errors.asJsonList().isEmpty())
                  break;
            }
            if (size < min || size > max)
               errors = maybeError(errors, Json.make("Array  " + param.toString(maxchars) +
                     " has number of elements outside of the permitted range [" + min + "," + max + "]."));
            return errors;
         }
      }

      class CheckPropertyPresent implements Instruction {
         String propname;

         public CheckPropertyPresent(String propname) {
            this.propname = propname;
         }

         public Json apply(Json param) {
            if (!param.isObject()) return null;
            if (param.has(propname)) return null;
            else return Json.array().add(Json.make("Required property " + propname +
                  " missing from object " + param.toString(maxchars)));
         }
      }

      class CheckObject implements Instruction {
         int min = 0, max = Integer.MAX_VALUE;
         Instruction additionalSchema = any;
         ArrayList<CheckProperty> props = new ArrayList<CheckProperty>();
         ArrayList<CheckPatternProperty> patternProps = new ArrayList<CheckPatternProperty>();

         // Object validation
         class CheckProperty implements Instruction {
            String name;
            Instruction schema;

            public CheckProperty(String name, Instruction schema) {
               this.name = name;
               this.schema = schema;
            }

            public Json apply(Json param) {
               Json value = param.at(name);
               if (value == null)
                  return null;
               else
                  return schema.apply(param.at(name));
            }
         }

         class CheckPatternProperty // implements Instruction
         {
            Pattern pattern;
            Instruction schema;

            public CheckPatternProperty(String pattern, Instruction schema) {
               this.pattern = Pattern.compile(pattern);
               this.schema = schema;
            }

            public Json apply(Json param, Set<String> found) {
               Json errors = null;
               for (Map.Entry<String, Json> e : param.asJsonMap().entrySet())
                  if (pattern.matcher(e.getKey()).find()) {
                     found.add(e.getKey());
                     errors = maybeError(errors, schema.apply(e.getValue()));
                  }
               return errors;
            }
         }

         public Json apply(Json param) {
            Json errors = null;
            if (!param.isObject()) return errors;
            HashSet<String> checked = new HashSet<String>();
            for (CheckProperty I : props) {
               if (param.has(I.name)) checked.add(I.name);
               errors = maybeError(errors, I.apply(param));
            }
            for (CheckPatternProperty I : patternProps) {

               errors = maybeError(errors, I.apply(param, checked));
            }
            if (additionalSchema != any) for (Map.Entry<String, Json> e : param.asJsonMap().entrySet())
               if (!checked.contains(e.getKey()))
                  errors = maybeError(errors, additionalSchema == null ?
                        Json.make("Extra property '" + e.getKey() +
                              "', schema doesn't allow any properties not explicitly defined:" +
                              param.toString(maxchars))
                        : additionalSchema.apply(e.getValue()));
            if (param.asJsonMap().size() < min)
               errors = maybeError(errors, Json.make("Object " + param.toString(maxchars) +
                     " has fewer than the permitted " + min + "  number of properties."));
            if (param.asJsonMap().size() > max)
               errors = maybeError(errors, Json.make("Object " + param.toString(maxchars) +
                     " has more than the permitted " + min + "  number of properties."));
            return errors;
         }
      }

      static class Sequence implements Instruction {
         ArrayList<Instruction> seq = new ArrayList<Instruction>();

         public Json apply(Json param) {
            Json errors = null;
            for (Instruction I : seq)
               errors = maybeError(errors, I.apply(param));
            return errors;
         }

         public Sequence add(Instruction I) {
            seq.add(I);
            return this;
         }
      }

      class CheckType implements Instruction {
         Json types;

         public CheckType(Json types) {
            this.types = types;
         }

         public Json apply(Json param) {
            String ptype = param.isString() ? "string" :
                  param.isObject() ? "object" :
                        param.isArray() ? "array" :
                              param.isNumber() ? "number" :
                                    param.isNull() ? "null" : "boolean";
            for (Json type : types.asJsonList())
               if (type.asString().equals(ptype))
                  return null;
               else if (type.asString().equals("integer") &&
                     param.isNumber() &&
                     param.asDouble() % 1 == 0)
                  return null;
            return Json.array().add(Json.make("Type mistmatch for " + param.toString(maxchars) +
                  ", allowed types: " + types));
         }
      }

      class CheckEnum implements Instruction {
         Json theenum;

         public CheckEnum(Json theenum) {
            this.theenum = theenum;
         }

         public Json apply(Json param) {
            for (Json option : theenum.asJsonList())
               if (param.equals(option))
                  return null;
            return Json.array().add("Element " + param.toString(maxchars) +
                  " doesn't match any of enumerated possibilities " + theenum);
         }
      }

      class CheckAny implements Instruction {
         ArrayList<Instruction> alternates = new ArrayList<Instruction>();
         Json schema;

         public Json apply(Json param) {
            for (Instruction I : alternates)
               if (I.apply(param) == null)
                  return null;
            return Json.array().add("Element " + param.toString(maxchars) +
                  " must conform to at least one of available sub-schemas " +
                  schema.toString(maxchars));
         }
      }

      class CheckOne implements Instruction {
         ArrayList<Instruction> alternates = new ArrayList<Instruction>();
         Json schema;

         public Json apply(Json param) {
            int matches = 0;
            Json errors = Json.array();
            for (Instruction I : alternates) {
               Json result = I.apply(param);
               if (result == null)
                  matches++;
               else
                  errors.add(result);
            }
            if (matches != 1) {
               return Json.array().add("Element " + param.toString(maxchars) +
                     " must conform to exactly one of available sub-schemas, but not more " +
                     schema.toString(maxchars)).add(errors);
            } else
               return null;
         }
      }

      class CheckNot implements Instruction {
         Instruction I;
         Json schema;

         public CheckNot(Instruction I, Json schema) {
            this.I = I;
            this.schema = schema;
         }

         public Json apply(Json param) {
            if (I.apply(param) != null)
               return null;
            else
               return Json.array().add("Element " + param.toString(maxchars) +
                     " must NOT conform to the schema " + schema.toString(maxchars));
         }
      }

      static class CheckSchemaDependency implements Instruction {
         Instruction schema;
         String property;

         public CheckSchemaDependency(String property, Instruction schema) {
            this.property = property;
            this.schema = schema;
         }

         public Json apply(Json param) {
            if (!param.isObject()) return null;
            else if (!param.has(property)) return null;
            else return (schema.apply(param));
         }
      }

      class CheckPropertyDependency implements Instruction {
         Json required;
         String property;

         public CheckPropertyDependency(String property, Json required) {
            this.property = property;
            this.required = required;
         }

         public Json apply(Json param) {
            if (!param.isObject()) return null;
            if (!param.has(property)) return null;
            else {
               Json errors = null;
               for (Json p : required.asJsonList())
                  if (!param.has(p.asString()))
                     errors = maybeError(errors, Json.make("Conditionally required property " + p +
                           " missing from object " + param.toString(maxchars)));
               return errors;
            }
         }
      }

      Instruction compile(Json S, Map<Json, Instruction> compiled) {
         Instruction result = compiled.get(S);
         if (result != null)
            return result;
         Sequence seq = new Sequence();
         compiled.put(S, seq);
         if (S.has("type") && !S.is("type", "any"))
            seq.add(new CheckType(S.at("type").isString() ?
                  Json.array().add(S.at("type")) : S.at("type")));
         if (S.has("enum"))
            seq.add(new CheckEnum(S.at("enum")));
         if (S.has("allOf")) {
            Sequence sub = new Sequence();
            for (Json x : S.at("allOf").asJsonList())
               sub.add(compile(x, compiled));
            seq.add(sub);
         }
         if (S.has("anyOf")) {
            CheckAny any = new CheckAny();
            any.schema = S.at("anyOf");
            for (Json x : any.schema.asJsonList())
               any.alternates.add(compile(x, compiled));
            seq.add(any);
         }
         if (S.has("oneOf")) {
            CheckOne any = new CheckOne();
            any.schema = S.at("oneOf");
            for (Json x : any.schema.asJsonList())
               any.alternates.add(compile(x, compiled));
            seq.add(any);
         }
         if (S.has("not"))
            seq.add(new CheckNot(compile(S.at("not"), compiled), S.at("not")));

         if (S.has("required") && S.at("required").isArray()) {
            for (Json p : S.at("required").asJsonList())
               seq.add(new CheckPropertyPresent(p.asString()));
         }
         CheckObject objectCheck = new CheckObject();
         if (S.has("properties"))
            for (Map.Entry<String, Json> p : S.at("properties").asJsonMap().entrySet())
               objectCheck.props.add(objectCheck.new CheckProperty(
                     p.getKey(), compile(p.getValue(), compiled)));
         if (S.has("patternProperties"))
            for (Map.Entry<String, Json> p : S.at("patternProperties").asJsonMap().entrySet())
               objectCheck.patternProps.add(objectCheck.new CheckPatternProperty(p.getKey(),
                     compile(p.getValue(), compiled)));
         if (S.has("additionalProperties")) {
            if (S.at("additionalProperties").isObject())
               objectCheck.additionalSchema = compile(S.at("additionalProperties"), compiled);
            else if (!S.at("additionalProperties").asBoolean())
               objectCheck.additionalSchema = null; // means no additional properties allowed
         }
         if (S.has("minProperties"))
            objectCheck.min = S.at("minProperties").asInteger();
         if (S.has("maxProperties"))
            objectCheck.max = S.at("maxProperties").asInteger();

         if (!objectCheck.props.isEmpty() || !objectCheck.patternProps.isEmpty() ||
               objectCheck.additionalSchema != any ||
               objectCheck.min > 0 || objectCheck.max < Integer.MAX_VALUE)
            seq.add(objectCheck);

         CheckArray arrayCheck = new CheckArray();
         if (S.has("items"))
            if (S.at("items").isObject())
               arrayCheck.schema = compile(S.at("items"), compiled);
            else {
               arrayCheck.schemas = new ArrayList<Instruction>();
               for (Json s : S.at("items").asJsonList())
                  arrayCheck.schemas.add(compile(s, compiled));
            }
         if (S.has("additionalItems"))
            if (S.at("additionalItems").isObject())
               arrayCheck.additionalSchema = compile(S.at("additionalItems"), compiled);
            else if (!S.at("additionalItems").asBoolean())
               arrayCheck.additionalSchema = null;
         if (S.has("uniqueItems"))
            arrayCheck.uniqueitems = S.at("uniqueItems").asBoolean();
         if (S.has("minItems"))
            arrayCheck.min = S.at("minItems").asInteger();
         if (S.has("maxItems"))
            arrayCheck.max = S.at("maxItems").asInteger();
         if (arrayCheck.schema != null || arrayCheck.schemas != null ||
               arrayCheck.additionalSchema != any ||
               arrayCheck.uniqueitems != null ||
               arrayCheck.max < Integer.MAX_VALUE || arrayCheck.min > 0)
            seq.add(arrayCheck);

         CheckNumber numberCheck = new CheckNumber();
         if (S.has("minimum"))
            numberCheck.min = S.at("minimum").asDouble();
         if (S.has("maximum"))
            numberCheck.max = S.at("maximum").asDouble();
         if (S.has("multipleOf"))
            numberCheck.multipleOf = S.at("multipleOf").asDouble();
         if (S.has("exclusiveMinimum"))
            numberCheck.exclusiveMin = S.at("exclusiveMinimum").asBoolean();
         if (S.has("exclusiveMaximum"))
            numberCheck.exclusiveMax = S.at("exclusiveMaximum").asBoolean();
         if (!Double.isNaN(numberCheck.min) || !Double.isNaN(numberCheck.max) || !Double.isNaN(numberCheck.multipleOf))
            seq.add(numberCheck);

         CheckString stringCheck = new CheckString();
         if (S.has("minLength"))
            stringCheck.min = S.at("minLength").asInteger();
         if (S.has("maxLength"))
            stringCheck.max = S.at("maxLength").asInteger();
         if (S.has("pattern"))
            stringCheck.pattern = Pattern.compile(S.at("pattern").asString());
         if (stringCheck.min > 0 || stringCheck.max < Integer.MAX_VALUE || stringCheck.pattern != null)
            seq.add(stringCheck);

         if (S.has("dependencies"))
            for (Map.Entry<String, Json> e : S.at("dependencies").asJsonMap().entrySet())
               if (e.getValue().isObject())
                  seq.add(new CheckSchemaDependency(e.getKey(), compile(e.getValue(), compiled)));
               else if (e.getValue().isArray())
                  seq.add(new CheckPropertyDependency(e.getKey(), e.getValue()));
               else
                  seq.add(new CheckPropertyDependency(e.getKey(), Json.array(e.getValue())));
         result = seq.seq.size() == 1 ? seq.seq.get(0) : seq;
         compiled.put(S, result);
         return result;
      }

      int maxchars = 50;
      URI uri;
      Json theschema;
      Instruction start;

      DefaultSchema(URI uri, Json theschema, Function<URI, Json> relativeReferenceResolver) {
         try {
            this.uri = uri == null ? new URI("") : uri;
            if (relativeReferenceResolver == null)
               relativeReferenceResolver = docuri -> {
                  try {
                     return Json.read(fetchContent(docuri.toURL()));
                  } catch (Exception ex) {
                     throw new RuntimeException(ex);
                  }
               };
            this.theschema = theschema.dup();
            this.theschema = expandReferences(this.theschema,
                  this.theschema,
                  this.uri,
                  new HashMap<String, Json>(),
                  new IdentityHashMap<Json, Json>(),
                  relativeReferenceResolver);
         } catch (Exception ex) {
            throw new RuntimeException(ex);
         }
         this.start = compile(this.theschema, new IdentityHashMap<Json, Instruction>());
      }

      public Json validate(Json document) {
         Json result = Json.object("ok", true);
         Json errors = start.apply(document);
         return errors == null ? result : result.set("errors", errors).set("ok", false);
      }

      public Json toJson() {
         return theschema;
      }

      public Json generate(Json options) {
         // TODO...
         return Json.nil();
      }
   }

   public static Schema schema(Json S) {
      return new DefaultSchema(null, S, null);
   }

   public static Schema schema(URI uri) {
      return schema(uri, null);
   }

   public static Schema schema(URI uri, Function<URI, Json> relativeReferenceResolver) {
      try {
         return new DefaultSchema(uri, Json.read(Json.fetchContent(uri.toURL())), relativeReferenceResolver);
      } catch (Exception ex) {
         throw new RuntimeException(ex);
      }
   }

   public static Schema schema(Json S, URI uri) {
      return new DefaultSchema(uri, S, null);
   }

   public static class DefaultFactory implements Factory {

      public Json nil() {
         return new NullJson();
      }

      public Json bool(boolean x) {
         return new BooleanJson(x ? Boolean.TRUE : Boolean.FALSE, null);
      }

      public Json string(String x) {
         return new StringJson(x, null);
      }

      public Json raw(String x) {
         return new RawJson(x, null);
      }

      public Json number(Number x) {
         return new NumberJson(x, null);
      }

      public Json array() {
         return new ArrayJson();
      }

      public Json object() {
         return new ObjectJson();
      }

      public Json make(Object anything) {
         if (anything == null)
            return nil();
         else if (anything instanceof Properties properties) {
            Json O = object();
            for (Map.Entry<?, ?> x : properties.entrySet()) {
               try {
                  O.set(x.getKey().toString(), factory().make(x.getValue()));
               } catch (IllegalArgumentException e) {
                  // Ignore unknown properties
               }
            }
            return O;
         } else if (anything instanceof Json)
            return (Json) anything;
         else if (anything instanceof String)
            return factory().string((String) anything);
         else if (anything instanceof JsonSerialization)
            return ((JsonSerialization) anything).toJson();
         else if (anything instanceof Class<?>)
            return factory().string(((Class<?>) anything).getName());
         else if (anything instanceof Collection<?>) {
            Json L = array();
            for (Object x : (Collection<?>) anything)
               L.add(factory().make(x));
            return L;
         } else if (anything instanceof Map<?, ?>) {
            Json O = object();
            for (Map.Entry<?, ?> x : ((Map<?, ?>) anything).entrySet())
               O.set(x.getKey().toString(), factory().make(x.getValue()));
            return O;
         } else if (anything instanceof Boolean)
            return factory().bool((Boolean) anything);
         else if (anything instanceof TimeQuantity)
            return factory().string(anything.toString());
         else if (anything instanceof Number)
            return factory().number((Number) anything);
         else if (anything instanceof Enum) {
            return factory().string(anything.toString());
         } else if (anything instanceof MediaType) {
            return factory().string(anything.toString());
         } else if (anything instanceof Instant) {
            return factory().string(anything.toString());
         } else if (anything.getClass().isArray()) {
            Class<?> comp = anything.getClass().getComponentType();
            if (!comp.isPrimitive())
               return Json.array((Object[]) anything);
            Json A = array();
            if (boolean.class == comp)
               for (boolean b : (boolean[]) anything) A.add(b);
            else if (byte.class == comp)
               for (byte b : (byte[]) anything) A.add(b);
            else if (char.class == comp)
               for (char b : (char[]) anything) A.add(b);
            else if (short.class == comp)
               for (short b : (short[]) anything) A.add(b);
            else if (int.class == comp)
               for (int b : (int[]) anything) A.add(b);
            else if (long.class == comp)
               for (long b : (long[]) anything) A.add(b);
            else if (float.class == comp)
               for (float b : (float[]) anything) A.add(b);
            else if (double.class == comp)
               for (double b : (double[]) anything) A.add(b);
            return A;
         } else if (anything instanceof Principal) {
            return factory().string(((Principal) anything).getName());
         } else
            throw new IllegalArgumentException("Don't know how to convert to Json : " + anything);
      }
   }

   public static final Factory defaultFactory = new DefaultFactory();

   private static Factory globalFactory = defaultFactory;

   // TODO: maybe use initialValue thread-local method to attach global factory by default here...
   private static final ThreadLocal<Factory> threadFactory = new ThreadLocal<Factory>();

   /**
    * <p>Return the {@link Factory} currently in effect. This is the factory that the {@link #make(Object)} method
    * will dispatch on upon determining the type of its argument. If you already know the type of element to construct,
    * you can avoid the type introspection implicit to the make method and call the factory directly. This will result
    * in an optimization. </p>
    *
    * @return the factory
    */
   public static Factory factory() {
      Factory f = threadFactory.get();
      return f != null ? f : globalFactory;
   }

   public static void escape(CharSequence sequence, Appendable out) throws IOException {
      escaper.escapeJsonString(sequence, out);
   }

   /**
    * <p>
    * Specify a global Json {@link Factory} to be used by all threads that don't have a specific thread-local factory
    * attached to them.
    * </p>
    *
    * @param factory The new global factory
    */
   public static void setGlobalFactory(Factory factory) {
      globalFactory = factory;
   }

   /**
    * <p>
    * Attach a thread-local Json {@link Factory} to be used specifically by this thread. Thread-local Json factories are
    * the only means to have different {@link Factory} implementations used simultaneously in the same application
    * (well, more accurately, the same ClassLoader).
    * </p>
    *
    * @param factory the new thread local factory
    */
   public static void attachFactory(Factory factory) {
      threadFactory.set(factory);
   }

   /**
    * <p>
    * Clear the thread-local factory previously attached to this thread via the {@link #attachFactory(Factory)} method.
    * The global factory takes effect after a call to this method.
    * </p>
    */
   public static void detachFactory() {
      threadFactory.remove();
   }

   /**
    * <p>
    * Parse a JSON entity from its string representation.
    * </p>
    *
    * @param jsonAsString A valid JSON representation as per the <a href="http://www.json.org">json.org</a> grammar.
    *                     Cannot be <code>null</code>.
    * @return The JSON entity parsed: an object, array, string, number or boolean, or null. Note that this method will
    * never return the actual Java <code>null</code>.
    */
   public static Json read(String jsonAsString) {
      return (Json) new Reader().read(jsonAsString);
   }

   /**
    * <p>
    * Parse a JSON entity from a <code>URL</code>.
    * </p>
    *
    * @param location A valid URL where to load a JSON document from. Cannot be <code>null</code>.
    * @return The JSON entity parsed: an object, array, string, number or boolean, or null. Note that this method will
    * never return the actual Java <code>null</code>.
    */
   public static Json read(URL location) {
      return (Json) new Reader().read(fetchContent(location));
   }

   /**
    * <p>
    * Parse a JSON entity from a {@link CharacterIterator}.
    * </p>
    *
    * @param it A character iterator.
    * @return the parsed JSON element
    * @see #read(String)
    */
   public static Json read(CharacterIterator it) {
      return (Json) new Reader().read(it);
   }

   /**
    * @return the <code>null Json</code> instance.
    */
   public static Json nil() {
      return factory().nil();
   }

   /**
    * @return a newly constructed, empty JSON object.
    */
   public static Json object() {
      return factory().object();
   }

   /**
    * <p>Return a new JSON object initialized from the passed list of
    * name/value pairs. The number of arguments must be even. Each argument at an even position is taken to be a name
    * for the following value. The name arguments are normally of type Java String, but they can be of any other type
    * having an appropriate
    * <code>toString</code> method. Each value is first converted
    * to a <code>Json</code> instance using the {@link #make(Object)} method.
    * </p>
    *
    * @param args A sequence of name value pairs.
    * @return the new JSON object.
    */
   public static Json object(Object... args) {
      Json j = object();
      if (args.length % 2 != 0)
         throw new IllegalArgumentException("An even number of arguments is expected.");
      for (int i = 0; i < args.length; i++)
         j.set(args[i].toString(), factory().make(args[++i]));
      return j;
   }

   /**
    * @return a new constructed, empty JSON array.
    */
   public static Json array() {
      return factory().array();
   }

   /**
    * <p>Return a new JSON array filled up with the list of arguments.</p>
    *
    * @param args The initial content of the array.
    * @return the new JSON array
    */
   public static Json array(Object... args) {
      Json A = array();
      for (Object x : args)
         A.add(factory().make(x));
      return A;
   }

   public static <T> Json array(Collection<T> collection) {
      Json jsonArray = array();
      for (T obj : collection) {
         jsonArray.add(factory().make(obj));
      }
      return jsonArray;
   }

   /**
    * <p>
    * Exposes some internal methods that are useful for {@link Json.Factory} implementations or other extension/layers
    * of the library.
    * </p>
    *
    * @author Borislav Iordanov
    */
   public static class help {
      /**
       * <p>
       * Perform JSON escaping so that ", &lt; &gt; etc. characters are properly encoded in the JSON string representation
       * before returning to the client code. This is useful when serializing property names or string values.
       * </p>
       */
      public static String escape(String string) {
         return escaper.escapeJsonString(string);
      }

      /**
       * <p>
       * Given a JSON Pointer, as per RFC 6901, return the nested JSON value within the <code>element</code> parameter.
       * </p>
       */
      public static Json resolvePointer(String pointer, Json element) {
         return Json.resolvePointer(pointer, element);
      }
   }

   /**
    * <p>
    * Convert an arbitrary Java instance to a {@link Json} instance.
    * </p>
    *
    * <p>
    * Maps, Collections and arrays are recursively copied where each of their elements concerted into <code>Json</code>
    * instances as well. The keys of a {@link Map} parameter are normally strings, but anything with a meaningful
    * <code>toString</code> implementation will work as well.
    * </p>
    *
    * @param anything Any Java object that the current JSON factory in effect is capable of handling.
    * @return The <code>Json</code>. This method will never return <code>null</code>. It will throw an
    * {@link IllegalArgumentException} if it doesn't know how to convert the argument to a <code>Json</code> instance.
    * @throws IllegalArgumentException when the concrete type of the parameter is unknown.
    */
   public static Json make(Object anything) {
      return factory().make(anything);
   }

   // end of static utility method section

   Json enclosing = null;
   int line;
   int column;

   protected Json() {
   }

   protected Json(Json enclosing) {
      this.enclosing = enclosing;
   }

   public Json location(int line, int column) {
      this.line = line;
      this.column = column;
      return this;
   }

   public int getLine() {
      return line;
   }

   public int getColumn() {
      return column;
   }

   /**
    * <p>Return a string representation of <code>this</code> that does
    * not exceed a certain maximum length. This is useful in constructing error messages or any other place where only a
    * "preview" of the JSON element should be displayed. Some JSON structures can get very large and this method will
    * help avoid string serializing the whole of them. </p>
    *
    * @param maxCharacters The maximum number of characters for the string representation.
    * @return The string representation of this object.
    */
   public String toString(int maxCharacters) {
      return toString();
   }

   public String toPrettyString() {
      return toString();
   }

   /**
    * <p>Explicitly set the parent of this element. The parent is presumably an array
    * or an object. Normally, there's no need to call this method as the parent is automatically set by the framework.
    * You may need to call it however, if you implement your own {@link Factory} with your own implementations of the
    * Json types.
    * </p>
    *
    * @param enclosing The parent element.
    */
   public void attachTo(Json enclosing) {
      this.enclosing = enclosing;
   }

   /**
    * @return the <code>Json</code> entity, if any, enclosing this
    * <code>Json</code>. The returned value can be <code>null</code> or
    * a <code>Json</code> object or list, but not one of the primitive types.
    */
   public final Json up() {
      return enclosing;
   }

   /**
    * @return a clone (a duplicate) of this <code>Json</code> entity. Note that cloning is deep if array and objects.
    * Primitives are also cloned, even though their values are immutable because the new enclosing entity (the result of
    * the {@link #up()} method) may be different. since they are immutable.
    */
   public Json dup() {
      return this;
   }

   /**
    * <p>Return the <code>Json</code> element at the specified index of this
    * <code>Json</code> array. This method applies only to Json arrays.
    * </p>
    *
    * @param index The index of the desired element.
    * @return The JSON element at the specified index in this array.
    */
   public Json at(int index) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Return the specified property of a <code>Json</code> object or <code>null</code> if there's no such property. This
    * method applies only to Json objects.
    * </p>
    *
    * @param property the property name.
    * @return The JSON element that is the value of that property.
    */
   public Json at(String property) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Return the specified property of a <code>Json</code> object if it exists. If it doesn't, then create a new
    * property with value the <code>def</code> parameter and return that parameter.
    * </p>
    *
    * @param property The property to return.
    * @param def      The default value to set and return in case the property doesn't exist.
    */
   public final Json at(String property, Json def) {
      Json x = at(property);
      if (x == null) {
         return def;
      } else
         return x;
   }

   /**
    * <p>
    * Return the specified property of a <code>Json</code> object if it exists. If it doesn't, then create a new
    * property with value the <code>def</code> parameter and return that parameter.
    * </p>
    *
    * @param property The property to return.
    * @param def      The default value to set and return in case the property doesn't exist.
    */
   public final Json at(String property, Object def) {
      return at(property, make(def));
   }

   /**
    * <p>
    * Return true if this <code>Json</code> object has the specified property and false otherwise.
    * </p>
    *
    * @param property The name of the property.
    */
   public boolean has(String property) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Return <code>true</code> if and only if this <code>Json</code> object has a property with the specified value. In
    * particular, if the object has no such property <code>false</code> is returned.
    * </p>
    *
    * @param property The property name.
    * @param value    The value to compare with. Comparison is done via the equals method. If the value is not an
    *                 instance of <code>Json</code>, it is first converted to such an instance.
    * @return
    */
   public boolean is(String property, Object value) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Return <code>true</code> if and only if this <code>Json</code> array has an element with the specified value at
    * the specified index. In particular, if the array has no element at this index, <code>false</code> is returned.
    * </p>
    *
    * @param index The 0-based index of the element in a JSON array.
    * @param value The value to compare with. Comparison is done via the equals method. If the value is not an instance
    *              of <code>Json</code>, it is first converted to such an instance.
    * @return
    */
   public boolean is(int index, Object value) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Add the specified <code>Json</code> element to this array.
    * </p>
    *
    * @return this
    */
   public Json add(Json el) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Add an arbitrary Java object to this <code>Json</code> array. The object is first converted to a <code>Json</code>
    * instance by calling the static {@link #make} method.
    * </p>
    *
    * @param anything Any Java object that can be converted to a Json instance.
    * @return this
    */
   public final Json add(Object anything) {
      return add(make(anything));
   }

   /**
    * <p>
    * Remove the specified property from a <code>Json</code> object and return that property.
    * </p>
    *
    * @param property The property to be removed.
    * @return The property value or <code>null</code> if the object didn't have such a property to begin with.
    */
   public Json atDel(String property) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Remove the element at the specified index from a <code>Json</code> array and return that element.
    * </p>
    *
    * @param index The index of the element to delete.
    * @return The element value.
    */
   public Json atDel(int index) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Delete the specified property from a <code>Json</code> object.
    * </p>
    *
    * @param property The property to be removed.
    * @return this
    */
   public Json delAt(String property) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Remove the element at the specified index from a <code>Json</code> array.
    * </p>
    *
    * @param index The index of the element to delete.
    * @return this
    */
   public Json delAt(int index) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Remove the specified element from a <code>Json</code> array.
    * </p>
    *
    * @param el The element to delete.
    * @return this
    */
   public Json remove(Json el) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Remove the specified Java object (converted to a Json instance) from a <code>Json</code> array. This is equivalent
    * to
    * <code>remove({@link #make(Object)})</code>.
    * </p>
    *
    * @param anything The object to delete.
    * @return this
    */
   public final Json remove(Object anything) {
      return remove(make(anything));
   }

   /**
    * <p>
    * Set a <code>Json</code> objects's property.
    * </p>
    *
    * @param property The property name.
    * @param value    The value of the property.
    * @return this
    */
   public Json set(String property, Json value) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Set a <code>Json</code> objects's property.
    * </p>
    *
    * @param property The property name.
    * @param value    The value of the property, converted to a <code>Json</code> representation with {@link #make}.
    * @return this
    */
   public final Json set(String property, Object value) {
      return set(property, make(value));
   }

   /**
    * <p>
    * Set a <code>Json</code> object's property only if the value is not null
    * </p>
    *
    * @param property The property name.
    * @param value    The value of the property, converted to a <code>Json</code> representation with {@link #make}.
    * @return this
    */
   public final Json setIfNotNull(String property, Object value) {
      if (value != null) {
         return set(property, make(value));
      } else {
         return this;
      }
   }

   /**
    * <p>
    * Change the value of a JSON array element. This must be an array.
    * </p>
    *
    * @param index 0-based index of the element in the array.
    * @param value the new value of the element
    * @return this
    */
   public Json set(int index, Object value) {
      throw new UnsupportedOperationException();
   }

   /**
    * <p>
    * Combine this object or array with the passed in object or array. The types of
    * <code>this</code> and the <code>object</code> argument must match. If both are
    * <code>Json</code> objects, all properties of the parameter are added to <code>this</code>.
    * If both are arrays, all elements of the parameter are appended to <code>this</code>
    * </p>
    *
    * @param object  The object or array whose properties or elements must be added to this Json object or array.
    * @param options A sequence of options that governs the merging process.
    * @return this
    */
   public Json with(Json object, Json[] options) {
      throw new UnsupportedOperationException();
   }

   /**
    * Same as <code>{}@link #with(Json,Json...options)}</code> with each option argument converted to <code>Json</code>
    * first.
    */
   public Json with(Json object, Object... options) {
      Json[] jopts = new Json[options.length];
      for (int i = 0; i < jopts.length; i++)
         jopts[i] = make(options[i]);
      return with(object, jopts);
   }

   public Json replace(Json oldJson, Json newJson) {
      if (this.isObject()) {
         for (Map.Entry<String, Json> entry : this.asJsonMap().entrySet()) {
            if (entry.getValue() == oldJson) {
               entry.setValue(newJson);
               return newJson;
            }
         }
         throw new IllegalArgumentException();
      } else {
         throw new UnsupportedOperationException();
      }
   }

   /**
    * @return the underlying value of this <code>Json</code> entity. The actual value will be a Java Boolean, String,
    * Number, Map, List or null. For complex entities (objects or arrays), the method will perform a deep copy and extra
    * underlying values recursively for all nested elements.
    */
   public Object getValue() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return the boolean value of a boolean <code>Json</code> instance. Call {@link #isBoolean()} first if you're not
    * sure this instance is indeed a boolean.
    */
   public boolean asBoolean() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return the string value of a string <code>Json</code> instance. Call {@link #isString()} first if you're not sure
    * this instance is indeed a string.
    */
   public String asString() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return the integer value of a number <code>Json</code> instance. Call {@link #isNumber()} first if you're not
    * sure this instance is indeed a number.
    */
   public int asInteger() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return the float value of a float <code>Json</code> instance. Call {@link #isNumber()} first if you're not sure
    * this instance is indeed a number.
    */
   public float asFloat() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return the double value of a number <code>Json</code> instance. Call {@link #isNumber()} first if you're not sure
    * this instance is indeed a number.
    */
   public double asDouble() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return the long value of a number <code>Json</code> instance. Call {@link #isNumber()} first if you're not sure
    * this instance is indeed a number.
    */
   public long asLong() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return the short value of a number <code>Json</code> instance. Call {@link #isNumber()} first if you're not sure
    * this instance is indeed a number.
    */
   public short asShort() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return the byte value of a number <code>Json</code> instance. Call {@link #isNumber()} first if you're not sure
    * this instance is indeed a number.
    */
   public byte asByte() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return the first character of a string <code>Json</code> instance. Call {@link #isString()} first if you're not
    * sure this instance is indeed a string.
    */
   public char asChar() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return a map of the properties of an object <code>Json</code> instance. The map is a clone of the object and can
    * be modified safely without affecting it. Call {@link #isObject()} first if you're not sure this instance is indeed
    * a
    * <code>Json</code> object.
    */
   public Map<String, Object> asMap() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return the underlying map of properties of a <code>Json</code> object. The returned map is the actual object
    * representation so any modifications to it are modifications of the <code>Json</code> object itself. Call
    * {@link #isObject()} first if you're not sure this instance is indeed a
    * <code>Json</code> object.
    */
   public Map<String, Json> asJsonMap() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return a list of the elements of a <code>Json</code> array. The list is a clone of the array and can be modified
    * safely without affecting it. Call {@link #isArray()} first if you're not sure this instance is indeed a
    * <code>Json</code> array.
    */
   public List<Object> asList() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return the underlying {@link List} representation of a <code>Json</code> array. The returned list is the actual
    * array representation so any modifications to it are modifications of the <code>Json</code> array itself. Call
    * {@link #isArray()} first if you're not sure this instance is indeed a
    * <code>Json</code> array.
    */
   public List<Json> asJsonList() {
      throw new UnsupportedOperationException();
   }

   /**
    * @return <code>true</code> if this is a <code>Json</code> null entity
    * and <code>false</code> otherwise.
    */
   public boolean isNull() {
      return false;
   }

   /**
    * @return <code>true</code> if this is a <code>Json</code> string entity
    * and <code>false</code> otherwise.
    */
   public boolean isString() {
      return false;
   }

   /**
    * @return <code>true</code> if this is a <code>Json</code> number entity
    * and <code>false</code> otherwise.
    */
   public boolean isNumber() {
      return false;
   }

   /**
    * @return <code>true</code> if this is a <code>Json</code> boolean entity
    * and <code>false</code> otherwise.
    */
   public boolean isBoolean() {
      return false;
   }

   /**
    * @return <code>true</code> if this is a <code>Json</code> array (i.e. list) entity
    * and <code>false</code> otherwise.
    */
   public boolean isArray() {
      return false;
   }

   public boolean isRaw() {
      return false;
   }

   /**
    * @return <code>true</code> if this is a <code>Json</code> object entity
    * and <code>false</code> otherwise.
    */
   public boolean isObject() {
      return false;
   }

   /**
    * @return <code>true</code> if this is a <code>Json</code> primitive entity
    * (one of string, number or boolean) and <code>false</code> otherwise.
    */
   public boolean isPrimitive() {
      return isString() || isNumber() || isBoolean();
   }

   /**
    * <p>
    * Json-pad this object as an argument to a callback function.
    * </p>
    *
    * @param callback The name of the callback function. Can be null or empty, in which case no padding is done.
    * @return The jsonpadded, stringified version of this object if the <code>callback</code> is not null or empty, or
    * just the stringified version of the object.
    */
   public String pad(String callback) {
      return (callback != null && !callback.isEmpty())
            ? callback + "(" + toString() + ");"
            : toString();
   }

   //-------------------------------------------------------------------------
   // END OF PUBLIC INTERFACE
   //-------------------------------------------------------------------------

   /**
    * Return an object representing the complete configuration of a merge. The properties of the object represent paths
    * of the JSON structure being merged and the values represent the set of options that apply to each path.
    *
    * @param options the configuration options
    * @return the configuration object
    */
   protected Json collectWithOptions(Json... options) {
      Json result = object();
      for (Json opt : options) {
         if (opt.isString()) {
            if (!result.has(""))
               result.set("", object());
            result.at("").set(opt.asString(), true);
         } else {
            if (!opt.has("for"))
               opt.set("for", array(""));
            Json forPaths = opt.at("for");
            if (!forPaths.isArray())
               forPaths = array(forPaths);
            for (Json path : forPaths.asJsonList()) {
               if (!result.has(path.asString()))
                  result.set(path.asString(), object());
               Json at_path = result.at(path.asString());
               at_path.set("merge", opt.is("merge", true));
               at_path.set("dup", opt.is("dup", true));
               at_path.set("sort", opt.is("sort", true));
               at_path.set("compareBy", opt.at("compareBy", nil()));
            }
         }
      }
      return result;
   }

   static class NullJson extends Json {
      private static final long serialVersionUID = 1L;

      NullJson() {
      }

      NullJson(Json e) {
         super(e);
      }

      public Object getValue() {
         return null;
      }

      public Json dup() {
         return new NullJson();
      }

      public boolean isNull() {
         return true;
      }

      public String toString() {
         return "null";
      }

      public List<Object> asList() {
         return (List<Object>) Collections.singletonList(null);
      }

      public int hashCode() {
         return 0;
      }

      public boolean equals(Object x) {
         return x instanceof NullJson;
      }
   }

   /**
    * <p>
    * Set the parent (i.e. enclosing element) of Json element.
    * </p>
    *
    * @param el
    * @param parent
    */
   static void setParent(Json el, Json parent) {
      if (el.enclosing == null)
         el.enclosing = parent;
      else if (el.enclosing instanceof ParentArrayJson)
         ((ParentArrayJson) el.enclosing).L.add(parent);
      else {
         ParentArrayJson A = new ParentArrayJson();
         A.L.add(el.enclosing);
         A.L.add(parent);
         el.enclosing = A;
      }
   }

   /**
    * <p>
    * Remove/unset the parent (i.e. enclosing element) of Json element.
    * </p>
    *
    * @param el
    * @param parent
    */
   static void removeParent(Json el, Json parent) {
      if (el.enclosing == parent)
         el.enclosing = null;
      else if (el.enclosing.isArray()) {
         ArrayJson A = (ArrayJson) el.enclosing;
         int idx = 0;
         while (A.L.get(idx) != parent && idx < A.L.size()) idx++;
         if (idx < A.L.size())
            A.L.remove(idx);
      }
   }

   static class BooleanJson extends Json {
      private static final long serialVersionUID = 1L;

      boolean val;

      BooleanJson() {
      }

      BooleanJson(Json e) {
         super(e);
      }

      BooleanJson(Boolean val, Json e) {
         super(e);
         this.val = val;
      }

      public Object getValue() {
         return val;
      }

      public Json dup() {
         return new BooleanJson(val, null);
      }

      public boolean asBoolean() {
         return val;
      }

      public boolean isBoolean() {
         return true;
      }

      public String toString() {
         return val ? "true" : "false";
      }

      @SuppressWarnings("unchecked")
      public List<Object> asList() {
         return (List<Object>) (List<?>) Collections.singletonList(val);
      }

      public int hashCode() {
         return val ? 1 : 0;
      }

      public boolean equals(Object x) {
         return x instanceof BooleanJson && ((BooleanJson) x).val == val;
      }
   }

   static class StringJson extends Json {
      private static final long serialVersionUID = 1L;

      String val;

      StringJson() {
      }

      StringJson(Json e) {
         super(e);
      }

      StringJson(String val, Json e) {
         super(e);
         this.val = val;
      }

      public Json dup() {
         return new StringJson(val, null);
      }

      public boolean isString() {
         return true;
      }

      public Object getValue() {
         return val;
      }

      public String asString() {
         return val;
      }

      public int asInteger() {
         return Integer.parseInt(val);
      }

      public float asFloat() {
         return Float.parseFloat(val);
      }

      public double asDouble() {
         return Double.parseDouble(val);
      }

      public long asLong() {
         return Long.parseLong(val);
      }

      public short asShort() {
         return Short.parseShort(val);
      }

      public byte asByte() {
         return Byte.parseByte(val);
      }

      public char asChar() {
         return val.charAt(0);
      }

      @SuppressWarnings("unchecked")
      public List<Object> asList() {
         return (List<Object>) (List<?>) Collections.singletonList(val);
      }

      public String toString() {
         return '"' + escaper.escapeJsonString(val) + '"';
      }

      public String toString(int maxCharacters) {
         if (val.length() <= maxCharacters)
            return toString();
         else
            return '"' + escaper.escapeJsonString(val.subSequence(0, maxCharacters)) + "...\"";
      }

      public int hashCode() {
         return val.hashCode();
      }

      public boolean equals(Object x) {
         return x instanceof StringJson && ((StringJson) x).val.equals(val);
      }
   }

   static class RawJson extends StringJson {
      public RawJson(String val, Json e) {
         super(val, e);
      }

      public String toString() {
         return val;
      }

      @Override
      public String toPrettyString() {
         return toPrettyStringImpl(1);
      }

      String toPrettyStringImpl(int ident) {
         return val;
      }

      @Override
      public boolean isRaw() {
         return true;
      }
   }

   static class NumberJson extends Json {
      private static final long serialVersionUID = 1L;

      Number val;

      NumberJson() {
      }

      NumberJson(Json e) {
         super(e);
      }

      NumberJson(Number val, Json e) {
         super(e);
         this.val = val;
      }

      public Json dup() {
         return new NumberJson(val, null);
      }

      public boolean isNumber() {
         return true;
      }

      public Object getValue() {
         return val;
      }

      public String asString() {
         return val.toString();
      }

      public int asInteger() {
         return val.intValue();
      }

      public float asFloat() {
         return val.floatValue();
      }

      public double asDouble() {
         return val.doubleValue();
      }

      public long asLong() {
         return val.longValue();
      }

      public short asShort() {
         return val.shortValue();
      }

      public byte asByte() {
         return val.byteValue();
      }

      @SuppressWarnings("unchecked")
      public List<Object> asList() {
         return (List<Object>) (List<?>) Collections.singletonList(val);
      }

      public String toString() {
         return val.toString();
      }

      public int hashCode() {
         return val.hashCode();
      }

      public boolean equals(Object x) {
         return x instanceof NumberJson && val.doubleValue() == ((NumberJson) x).val.doubleValue();
      }
   }

   static class ArrayJson extends Json {
      private static final long serialVersionUID = 1L;

      List<Json> L = new ArrayList<Json>();

      ArrayJson() {
      }

      ArrayJson(Json e) {
         super(e);
      }


      public Json dup() {
         ArrayJson j = new ArrayJson();
         for (Json e : L) {
            Json v = e.dup();
            v.enclosing = j;
            j.L.add(v);
         }
         return j;
      }

      public Json set(int index, Object value) {
         Json jvalue = make(value);
         L.set(index, jvalue);
         setParent(jvalue, this);
         return this;
      }

      public List<Json> asJsonList() {
         return L;
      }

      public List<Object> asList() {
         ArrayList<Object> A = new ArrayList<Object>();
         for (Json x : L)
            A.add(x.getValue());
         return A;
      }

      public boolean is(int index, Object value) {
         if (index < 0 || index >= L.size())
            return false;
         else
            return L.get(index).equals(make(value));
      }

      public Object getValue() {
         return asList();
      }

      public boolean isArray() {
         return true;
      }

      public Json at(int index) {
         return L.get(index);
      }

      public Json add(Json el) {
         L.add(el);
         setParent(el, this);
         return this;
      }

      public Json remove(Json el) {
         L.remove(el);
         el.enclosing = null;
         return this;
      }

      boolean isEqualJson(Json left, Json right) {
         if (left == null)
            return right == null;
         else
            return left.equals(right);
      }

      boolean isEqualJson(Json left, Json right, Json fields) {
         if (fields.isNull())
            return left.equals(right);
         else if (fields.isString())
            return isEqualJson(resolvePointer(fields.asString(), left),
                  resolvePointer(fields.asString(), right));
         else if (fields.isArray()) {
            for (Json field : fields.asJsonList())
               if (!isEqualJson(resolvePointer(field.asString(), left),
                     resolvePointer(field.asString(), right)))
                  return false;
            return true;
         } else
            throw new IllegalArgumentException("Compare by options should be either a property name or an array of property names: " + fields);
      }

      @SuppressWarnings({"unchecked", "rawtypes"})
      int compareJson(Json left, Json right, Json fields) {
         if (fields.isNull())
            return ((Comparable) left.getValue()).compareTo(right.getValue());
         else if (fields.isString()) {
            Json leftProperty = resolvePointer(fields.asString(), left);
            Json rightProperty = resolvePointer(fields.asString(), right);
            return ((Comparable) leftProperty).compareTo(rightProperty);
         } else if (fields.isArray()) {
            for (Json field : fields.asJsonList()) {
               Json leftProperty = resolvePointer(field.asString(), left);
               Json rightProperty = resolvePointer(field.asString(), right);
               int result = ((Comparable) leftProperty).compareTo(rightProperty);
               if (result != 0)
                  return result;
            }
            return 0;
         } else
            throw new IllegalArgumentException("Compare by options should be either a property name or an array of property names: " + fields);
      }

      Json withOptions(Json array, Json allOptions, String path) {
         Json opts = allOptions.at(path, object());
         boolean dup = opts.is("dup", true);
         Json compareBy = opts.at("compareBy", nil());
         if (opts.is("sort", true)) {
            int thisIndex = 0, thatIndex = 0;
            while (thatIndex < array.asJsonList().size()) {
               Json thatElement = array.at(thatIndex);
               if (thisIndex == L.size()) {
                  L.add(dup ? thatElement.dup() : thatElement);
                  thisIndex++;
                  thatIndex++;
                  continue;
               }
               int compared = compareJson(at(thisIndex), thatElement, compareBy);
               if (compared < 0) // this < that
                  thisIndex++;
               else if (compared > 0) // this > that
               {
                  L.add(thisIndex, dup ? thatElement.dup() : thatElement);
                  thatIndex++;
               } else { // equal, ignore
                  thatIndex++;
               }
            }
         } else {
            for (Json thatElement : array.asJsonList()) {
               boolean present = false;
               for (Json thisElement : L)
                  if (isEqualJson(thisElement, thatElement, compareBy)) {
                     present = true;
                     break;
                  }
               if (!present)
                  L.add(dup ? thatElement.dup() : thatElement);
            }
         }
         return this;
      }

      public Json with(Json object, Json... options) {
         if (object == null) return this;
         if (!object.isArray())
            add(object);
         else if (options.length > 0) {
            Json O = collectWithOptions(options);
            return withOptions(object, O, "");
         } else
            // what about "enclosing" here? we don't have a provision where a Json
            // element belongs to more than one enclosing elements...
            L.addAll(((ArrayJson) object).L);
         return this;
      }

      public Json atDel(int index) {
         Json el = L.remove(index);
         if (el != null)
            el.enclosing = null;
         return el;
      }

      public Json delAt(int index) {
         Json el = L.remove(index);
         if (el != null)
            el.enclosing = null;
         return this;
      }

      public String toString() {
         return toString(Integer.MAX_VALUE);
      }

      public String toString(int maxCharacters) {
         return toStringImpl(maxCharacters, new IdentityHashMap<Json, Json>());
      }

      String toPrettyStringImpl(int ident) {
         StringBuilder sb = new StringBuilder("[ ");
         for (Iterator<Json> i = L.iterator(); i.hasNext(); ) {
            Json value = i.next();
            String s = value.isObject() ? ((ObjectJson) value).toPrettyStringImpl(ident)
                  : value.isArray() ? ((ArrayJson) value).toPrettyStringImpl(ident)
                  : value.toString();
            sb.append(s);
            if (i.hasNext())
               sb.append(",");
         }
         sb.append(" ]");
         return sb.toString();
      }

      String toStringImpl(int maxCharacters, Map<Json, Json> done) {
         StringBuilder sb = new StringBuilder("[");
         for (Iterator<Json> i = L.iterator(); i.hasNext(); ) {
            Json value = i.next();
            String s = value.isObject() ? ((ObjectJson) value).toStringImpl(maxCharacters, done)
                  : value.isArray() ? ((ArrayJson) value).toStringImpl(maxCharacters, done)
                  : value.toString(maxCharacters);
            if (sb.length() + s.length() > maxCharacters)
               s = s.substring(0, Math.max(0, maxCharacters - sb.length()));
            else
               sb.append(s);
            if (i.hasNext())
               sb.append(",");
            if (sb.length() >= maxCharacters) {
               sb.append("...");
               break;
            }
         }
         sb.append("]");
         return sb.toString();
      }

      public int hashCode() {
         return L.hashCode();
      }

      public boolean equals(Object x) {
         return x instanceof ArrayJson && ((ArrayJson) x).L.equals(L);
      }
   }

   static class ParentArrayJson extends ArrayJson {

      /**
       *
       */
      private static final long serialVersionUID = 1L;

   }

   static class ObjectJson extends Json {
      private static final long serialVersionUID = 1L;

      Map<String, Json> object = new LinkedHashMap<>();

      ObjectJson() {
      }

      ObjectJson(Json e) {
         super(e);
      }

      public Json dup() {
         ObjectJson j = new ObjectJson();
         for (Map.Entry<String, Json> e : object.entrySet()) {
            Json v = e.getValue().dup();
            v.enclosing = j;
            j.object.put(e.getKey(), v);
         }
         return j;
      }

      public boolean has(String property) {
         return object.containsKey(property);
      }

      public boolean is(String property, Object value) {
         Json p = object.get(property);
         if (p == null)
            return false;
         else
            return p.equals(make(value));
      }

      public Json at(String property) {
         return object.get(property);
      }

      protected Json withOptions(Json other, Json allOptions, String path) {
         if (!allOptions.has(path))
            allOptions.set(path, object());
         Json options = allOptions.at(path, object());
         boolean duplicate = options.is("dup", true);
         if (options.is("merge", true)) {
            for (Map.Entry<String, Json> e : other.asJsonMap().entrySet()) {
               Json local = object.get(e.getKey());
               if (local instanceof ObjectJson)
                  ((ObjectJson) local).withOptions(e.getValue(), allOptions, path + "/" + e.getKey());
               else if (local instanceof ArrayJson)
                  ((ArrayJson) local).withOptions(e.getValue(), allOptions, path + "/" + e.getKey());
               else
                  set(e.getKey(), duplicate ? e.getValue().dup() : e.getValue());
            }
         } else if (duplicate)
            for (Map.Entry<String, Json> e : other.asJsonMap().entrySet())
               set(e.getKey(), e.getValue().dup());
         else
            for (Map.Entry<String, Json> e : other.asJsonMap().entrySet())
               set(e.getKey(), e.getValue());
         return this;
      }

      public Json with(Json x, Json... options) {
         if (x == null) return this;
         if (!x.isObject())
            throw new UnsupportedOperationException();
         if (options.length > 0) {
            Json O = collectWithOptions(options);
            return withOptions(x, O, "");
         } else for (Map.Entry<String, Json> e : x.asJsonMap().entrySet())
            set(e.getKey(), e.getValue());
         return this;
      }

      public Json set(String property, Json el) {
         if (property == null)
            throw new IllegalArgumentException("Null property names are not allowed, value is " + el);
         if (el == null)
            el = nil();
         setParent(el, this);
         object.put(property, el);
         return this;
      }

      public Json atDel(String property) {
         Json el = object.remove(property);
         removeParent(el, this);
         return el;
      }

      public Json delAt(String property) {
         Json el = object.remove(property);
         removeParent(el, this);
         return this;
      }

      public Object getValue() {
         return asMap();
      }

      public boolean isObject() {
         return true;
      }

      public Map<String, Object> asMap() {
         HashMap<String, Object> m = new HashMap<String, Object>();
         for (Map.Entry<String, Json> e : object.entrySet())
            m.put(e.getKey(), e.getValue().getValue());
         return m;
      }

      @Override
      public Map<String, Json> asJsonMap() {
         return object;
      }

      public String toString() {
         return toString(Integer.MAX_VALUE);
      }

      public String toString(int maxCharacters) {
         return toStringImpl(maxCharacters, new IdentityHashMap<Json, Json>());
      }

      public String toPrettyString() {
         return toPrettyStringImpl(1);
      }

      String toPrettyStringImpl(int ident) {
         StringBuilder sb = new StringBuilder("{");
         sb.append("\n");

         for (Iterator<Map.Entry<String, Json>> i = object.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry<String, Json> x = i.next();
            sb.append("  ".repeat(Math.max(0, ident)));
            sb.append('"');
            sb.append(escaper.escapeJsonString(x.getKey()));
            sb.append('"');
            sb.append(" : ");
            String s = x.getValue().isObject() ? ((ObjectJson) x.getValue()).toPrettyStringImpl(ident + 1)
                  : x.getValue().isArray() ? ((ArrayJson) x.getValue()).toPrettyStringImpl(ident + 1)
                  : x.getValue().isRaw() ? ((RawJson) x.getValue()).toPrettyStringImpl(ident + 1)
                  : x.getValue().toString();
            sb.append(s);
            if (i.hasNext()) {
               sb.append(",");
               sb.append("\n");
            }

         }

         sb.append("\n");
         ident -= 1;
         sb.append("  ".repeat(Math.max(0, ident)));

         sb.append("}");
         return sb.toString();
      }

      String toStringImpl(int maxCharacters, Map<Json, Json> done) {
         StringBuilder sb = new StringBuilder("{");
         if (done.containsKey(this))
            return sb.append("...}").toString();
         done.put(this, this);
         for (Iterator<Map.Entry<String, Json>> i = object.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry<String, Json> x = i.next();
            sb.append('"');
            sb.append(escaper.escapeJsonString(x.getKey()));
            sb.append('"');
            sb.append(":");
            String s = x.getValue().isObject() ? ((ObjectJson) x.getValue()).toStringImpl(maxCharacters, done)
                  : x.getValue().isArray() ? ((ArrayJson) x.getValue()).toStringImpl(maxCharacters, done)
                  : x.getValue().toString(maxCharacters);
            if (sb.length() + s.length() > maxCharacters)
               s = s.substring(0, Math.max(0, maxCharacters - sb.length()));
            sb.append(s);
            if (i.hasNext())
               sb.append(",");
            if (sb.length() >= maxCharacters) {
               sb.append("...");
               break;
            }
         }
         sb.append("}");
         return sb.toString();
      }

      public int hashCode() {
         return object.hashCode();
      }

      public boolean equals(Object x) {
         return x instanceof ObjectJson && ((ObjectJson) x).object.equals(object);
      }
   }

   // ------------------------------------------------------------------------
   // Extra utilities, taken from around the internet:
   // ------------------------------------------------------------------------

   /*
    * Copyright (C) 2008 Google Inc.
    *
    * Licensed under the Apache License, Version 2.0 (the "License");
    * you may not use this file except in compliance with the License.
    * You may obtain a copy of the License at
    *
    * http://www.apache.org/licenses/LICENSE-2.0
    *
    * Unless required by applicable law or agreed to in writing, software
    * distributed under the License is distributed on an "AS IS" BASIS,
    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    * See the License for the specific language governing permissions and
    * limitations under the License.
    */

   /**
    * A utility class that is used to perform JSON escaping so that ", <, >, etc. characters are properly encoded in the
    * JSON string representation before returning to the client code.
    *
    * <p>This class contains a single method to escape a passed in string value:
    * <pre>
    *   String jsonStringValue = "beforeQuote\"afterQuote";
    *   String escapedValue = Escaper.escapeJsonString(jsonStringValue);
    * </pre></p>
    *
    * @author Inderjeet Singh
    * @author Joel Leitch
    */
   static Escaper escaper = new Escaper();

   static final class Escaper {

      private static final char[] HEX_CHARS = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
      };

      public String escapeJsonString(CharSequence plainText) {
         StringBuilder escapedString = new StringBuilder(plainText.length() + 20);
         try {
            escapeJsonString(plainText, escapedString);
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
         return escapedString.toString();
      }

      private void escapeJsonString(CharSequence plainText, Appendable out) throws IOException {
         int pos = 0;  // Index just past the last char in plainText written to out.
         int len = plainText.length();

         for (int charCount, i = 0; i < len; i += charCount) {
            int codePoint = Character.codePointAt(plainText, i);
            charCount = Character.charCount(codePoint);

            if (!isControlCharacter(codePoint) && !mustEscapeCharInJsString(codePoint)) {
               continue;
            }

            out.append(plainText, pos, i);
            pos = i + charCount;
            switch (codePoint) {
               case '\b':
                  out.append("\\b");
                  break;
               case '\t':
                  out.append("\\t");
                  break;
               case '\n':
                  out.append("\\n");
                  break;
               case '\f':
                  out.append("\\f");
                  break;
               case '\r':
                  out.append("\\r");
                  break;
               case '\\':
                  out.append("\\\\");
                  break;
               case '/':
                  out.append("\\/");
                  break;
               case '"':
                  out.append("\\\"");
                  break;
               default:
                  appendHexJavaScriptRepresentation(codePoint, out);
                  break;
            }
         }
         out.append(plainText, pos, len);
      }

      private boolean mustEscapeCharInJsString(int codepoint) {
         char c = (char) codepoint;
         return c == '"' || c == '\\';
      }

      private static boolean isControlCharacter(int codePoint) {
         // JSON spec defines these code points as control characters, so they must be escaped
         return codePoint < 0x20
               || codePoint == 0x2028  // Line separator
               || codePoint == 0x2029  // Paragraph separator
               || (codePoint >= 0x7f && codePoint <= 0x9f);
      }

      private static void appendHexJavaScriptRepresentation(int codePoint, Appendable out)
            throws IOException {
         if (Character.isSupplementaryCodePoint(codePoint)) {
            // Handle supplementary unicode values which are not representable in
            // javascript.  We deal with these by escaping them as two 4B sequences
            // so that they will round-trip properly when sent from java to javascript
            // and back.
            char[] surrogates = Character.toChars(codePoint);
            appendHexJavaScriptRepresentation(surrogates[0], out);
            appendHexJavaScriptRepresentation(surrogates[1], out);
            return;
         }
         out.append("\\u")
               .append(HEX_CHARS[(codePoint >>> 12) & 0xf])
               .append(HEX_CHARS[(codePoint >>> 8) & 0xf])
               .append(HEX_CHARS[(codePoint >>> 4) & 0xf])
               .append(HEX_CHARS[codePoint & 0xf]);
      }
   }

   public static class MalformedJsonException extends RuntimeException {
      private static final long serialVersionUID = 1L;
      private final int row;
      private final int column;


      public MalformedJsonException(String msg, int row, int column) {
         super(msg + " at [" + row + "," + column + "]");
         this.row = row;
         this.column = column;
      }

      public int getRow() {
         return row;
      }

      public int getColumn() {
         return column;
      }
   }

   private static class Reader {
      private static final Object OBJECT_END = "}";
      private static final Object ARRAY_END = "]";
      private static final Object OBJECT_START = "{";
      private static final Object ARRAY_START = "[";
      private static final Object COLON = ":";
      private static final Object COMMA = ",";
      private static final HashSet<Object> PUNCTUATION = new HashSet<Object>(
            Arrays.asList(OBJECT_END, OBJECT_START, ARRAY_END, ARRAY_START, COLON, COMMA));
      public static final int FIRST = 0;
      public static final int CURRENT = 1;
      public static final int NEXT = 2;

      private static final Map<Character, Character> escapes = new HashMap<>();

      static {
         escapes.put('"', '"');
         escapes.put('\\', '\\');
         escapes.put('/', '/');
         escapes.put('b', '\b');
         escapes.put('f', '\f');
         escapes.put('n', '\n');
         escapes.put('r', '\r');
         escapes.put('t', '\t');
      }

      private CharacterIterator it;
      private char c;
      private Object token;
      private final StringBuilder buf = new StringBuilder();

      private int line = 1;
      private int col = 1;

      private char next() {
         if (it.getIndex() == it.getEndIndex())
            throw new MalformedJsonException("Reached end of input", line, col);
         c = it.next();
         if (c == '\n') {
            line++;
            col = 1;
         } else {
            col++;
         }
         return c;
      }

      private void previous() {
         c = it.previous();
         col--; // The parser doesn't backtrack through line-feeds, so we can avoid updating the row
      }

      private void skipWhiteSpace() {
         do {
            if (c == '/') {
               next();
               if (c == '*') {
                  // skip multiline comments
                  while (c != CharacterIterator.DONE)
                     if (next() == '*' && next() == '/')
                        break;
                  if (c == CharacterIterator.DONE)
                     throw new MalformedJsonException("Unterminated comment while parsing JSON string.", line, col);
               } else if (c == '/')
                  while (c != '\n' && c != CharacterIterator.DONE)
                     next();
               else {
                  previous();
                  break;
               }
            } else if (!Character.isWhitespace(c))
               break;
         } while (next() != CharacterIterator.DONE);
      }

      public Object read(CharacterIterator ci, int start) {
         it = ci;
         switch (start) {
            case FIRST:
               c = it.first();
               break;
            case CURRENT:
               c = it.current();
               break;
            case NEXT:
               c = it.next();
               break;
         }
         return read();
      }

      public Object read(CharacterIterator it) {
         return read(it, NEXT);
      }

      public Object read(String string) {
         return read(new StringCharacterIterator(string), FIRST);
      }

      private void expected(Object expectedToken, Object actual) {
         if (expectedToken != actual)
            throw new MalformedJsonException("Expected " + expectedToken + ", but got " + actual + " instead", line, col);
      }

      @SuppressWarnings("unchecked")
      private <T> T read() {
         skipWhiteSpace();
         char ch = c;
         // We save the current location before reading the next token
         int oLine = line;
         int oCol = col;
         next();
         switch (ch) {
            case '"':
               token = readString();
               break;
            case '[':
               token = readArray();
               break;
            case ']':
               token = ARRAY_END;
               break;
            case ',':
               token = COMMA;
               break;
            case '{':
               token = readObject(oLine, oCol);
               break;
            case '}':
               token = OBJECT_END;
               break;
            case ':':
               token = COLON;
               break;
            case 't':
               if (c != 'r' || next() != 'u' || next() != 'e')
                  throw new MalformedJsonException("Invalid JSON token: expected 'true' keyword.", line, col);
               next();
               token = factory().bool(Boolean.TRUE);
               break;
            case 'f':
               if (c != 'a' || next() != 'l' || next() != 's' || next() != 'e')
                  throw new MalformedJsonException("Invalid JSON token: expected 'false' keyword.", line, col);
               next();
               token = factory().bool(Boolean.FALSE);
               break;
            case 'n':
               if (c != 'u' || next() != 'l' || next() != 'l')
                  throw new MalformedJsonException("Invalid JSON token: expected 'null' keyword.", line, col);
               next();
               token = nil();
               break;
            default:
               c = it.previous();
               if (Character.isDigit(c) || c == '-') {
                  token = readNumber();
               } else throw new MalformedJsonException("Invalid JSON", line, col);
         }
         return (T) token;
      }

      private Json readObjectKey() {
         Object key = read();
         if (key == null)
            throw new MalformedJsonException("Missing object key (don't forget to put quotes!).", line, col);
         else if (key == OBJECT_END)
            return null;
         else if (PUNCTUATION.contains(key))
            throw new MalformedJsonException("Missing object key, found: " + key, line, col);
         else
            return (Json) key;
      }

      private Json readObject(int oLine, int oCol) {
         Json ret = object().location(oLine, oCol);
         Json key = readObjectKey();
         while (token != OBJECT_END) {
            expected(COLON, read()); // should be a colon
            if (token != OBJECT_END) {
               Json value = read();
               // We set the location of the value to that of the key
               value.location(key.getLine(), key.getColumn());
               ret.set(key.asString(), value);
               if (read() == COMMA) {
                  key = readObjectKey();
                  if (key == null || PUNCTUATION.contains(key.asString()))
                     throw new MalformedJsonException("Expected a property name, but found: " + key, line, col);
               } else
                  expected(OBJECT_END, token);
            }
         }
         return ret;
      }

      private Json readArray() {
         Json ret = array().location(line, col);
         Object value = read();
         while (token != ARRAY_END) {
            if (PUNCTUATION.contains(value))
               throw new MalformedJsonException("Expected array element, but found: " + value, line, col);
            ret.add((Json) value);
            if (read() == COMMA) {
               value = read();
               if (value == ARRAY_END)
                  throw new MalformedJsonException("Expected array element, but found end of array after command.", line, col);
            } else
               expected(ARRAY_END, token);
         }
         return ret;
      }

      private Json readNumber() {
         int nLine = line;
         int nCol = col;
         int length = 0;
         boolean isFloatingPoint = false;
         buf.setLength(0);

         if (c == '-') {
            add();
         }
         length += addDigits();
         if (c == '.') {
            add();
            length += addDigits();
            isFloatingPoint = true;
         }
         if (c == 'e' || c == 'E') {
            add();
            if (c == '+' || c == '-') {
               add();
            }
            addDigits();
            isFloatingPoint = true;
         }

         String s = buf.toString();
         Number n = isFloatingPoint
               ? (length < 17) ? Double.valueOf(s) : new BigDecimal(s)
               : (length < 20) ? Long.valueOf(s) : new BigInteger(s);
         return factory().number(n).location(nLine, nCol);
      }

      private int addDigits() {
         int ret;
         for (ret = 0; Character.isDigit(c); ++ret) {
            add();
         }
         return ret;
      }

      private Json readString() {
         int nLine = line;
         int nCol = col;
         buf.setLength(0);
         while (c != '"') {
            if (c == '\\') {
               next();
               if (c == 'u') {
                  add(unicode());
               } else {
                  Character value = escapes.get(c);
                  if (value != null) {
                     add(value);
                  }
               }
            } else {
               add();
            }
         }
         next();
         return factory().string(buf.toString()).location(nLine, nCol);
      }

      private void add(char cc) {
         buf.append(cc);
         next();
      }

      private void add() {
         add(c);
      }

      private char unicode() {
         int value = 0;
         for (int i = 0; i < 4; ++i) {
            switch (next()) {
               case '0':
               case '1':
               case '2':
               case '3':
               case '4':
               case '5':
               case '6':
               case '7':
               case '8':
               case '9':
                  value = (value << 4) + c - '0';
                  break;
               case 'a':
               case 'b':
               case 'c':
               case 'd':
               case 'e':
               case 'f':
                  value = (value << 4) + (c - 'a') + 10;
                  break;
               case 'A':
               case 'B':
               case 'C':
               case 'D':
               case 'E':
               case 'F':
                  value = (value << 4) + (c - 'A') + 10;
                  break;
            }
         }
         return (char) value;
      }
   }
   // END Reader

   public static void main(String[] argv) {
      try {
         URI assetUri = new URI("https://raw.githubusercontent.com/pudo/aleph/master/aleph/schema/entity/asset.json");
         URI schemaRoot = new URI("https://raw.githubusercontent.com/pudo/aleph/master/aleph/schema/");

         // This fails
         Json.schema(assetUri);

         // And so does this
         Json asset = Json.read(assetUri.toURL());
         Json.schema(asset, schemaRoot);
      } catch (Throwable t) {
         t.printStackTrace();
      }
   }
}
