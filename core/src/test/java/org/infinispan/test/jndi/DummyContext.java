/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
package org.infinispan.test.jndi;

import javax.naming.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;

public class DummyContext implements Context {


   ConcurrentHashMap<String, Object> bindings = new ConcurrentHashMap<String, Object>();
   boolean serializing;

   public DummyContext() {
      this.serializing = false;
   }

   public DummyContext(boolean serializing) {
      this.serializing = serializing;
   }

   /**
    * Retrieves the named object. If <tt>name</tt> is empty, returns a new instance of this context (which represents
    * the same naming context as this context, but its environment may be modified independently and it may be accessed
    * concurrently).
    *
    * @param name the name of the object to look up
    * @return the object bound to <tt>name</tt>
    * @throws NamingException if a naming exception is encountered
    * @see #lookup(String)
    * @see #lookupLink(Name)
    */
   public Object lookup(Name name) throws NamingException {
      return null;
   }

   /**
    * Retrieves the named object. See {@link #lookup(Name)} for details.
    *
    * @param name the name of the object to look up
    * @return the object bound to <tt>name</tt>
    * @throws NamingException if a naming exception is encountered
    */
   public Object lookup(String name) throws NamingException {
      try {
         deserialize();
         return bindings.get(name);
      }
      finally {
         serialize();
      }
   }

   /**
    * Binds a name to an object. All intermediate contexts and the target context (that named by all but terminal atomic
    * component of the name) must already exist.
    *
    * @param name the name to bind; may not be empty
    * @param obj  the object to bind; possibly null
    * @throws javax.naming.NameAlreadyBoundException
    *                         if name is already bound
    * @throws javax.naming.directory.InvalidAttributesException
    *                         if object did not supply all mandatory attributes
    * @throws NamingException if a naming exception is encountered
    * @see #bind(String,Object)
    * @see #rebind(Name,Object)
    * @see javax.naming.directory.DirContext#bind(Name,Object, javax.naming.directory.Attributes)
    */
   public void bind(Name name, Object obj) throws NamingException {
      bind("NAME: " + name.toString(), obj);
   }

   /**
    * Binds a name to an object. See {@link #bind(Name,Object)} for details.
    *
    * @param name the name to bind; may not be empty
    * @param obj  the object to bind; possibly null
    * @throws javax.naming.NameAlreadyBoundException
    *                         if name is already bound
    * @throws javax.naming.directory.InvalidAttributesException
    *                         if object did not supply all mandatory attributes
    * @throws NamingException if a naming exception is encountered
    */
   public void bind(String name, Object obj) throws NamingException {
      try {
         deserialize();
         bindings.put(name, obj);
      }
      finally {
         serialize();
      }
   }

   /**
    * Binds a name to an object, overwriting any existing binding. All intermediate contexts and the target context
    * (that named by all but terminal atomic component of the name) must already exist.
    * <p/>
    * <p> If the object is a <tt>DirContext</tt>, any existing attributes associated with the name are replaced with
    * those of the object. Otherwise, any existing attributes associated with the name remain unchanged.
    *
    * @param name the name to bind; may not be empty
    * @param obj  the object to bind; possibly null
    * @throws javax.naming.directory.InvalidAttributesException
    *                         if object did not supply all mandatory attributes
    * @throws NamingException if a naming exception is encountered
    * @see #rebind(String,Object)
    * @see #bind(Name,Object)
    * @see javax.naming.directory.DirContext#rebind(Name,Object, javax.naming.directory.Attributes)
    * @see javax.naming.directory.DirContext
    */
   public void rebind(Name name, Object obj) throws NamingException {
      bind(name, obj);
   }

   /**
    * Binds a name to an object, overwriting any existing binding. See {@link #rebind(Name,Object)} for details.
    *
    * @param name the name to bind; may not be empty
    * @param obj  the object to bind; possibly null
    * @throws javax.naming.directory.InvalidAttributesException
    *                         if object did not supply all mandatory attributes
    * @throws NamingException if a naming exception is encountered
    */
   public void rebind(String name, Object obj) throws NamingException {
      bind(name, obj);
   }

   /**
    * Unbinds the named object. Removes the terminal atomic name in <code>name</code> from the target context--that
    * named by all but the terminal atomic part of <code>name</code>.
    * <p/>
    * <p> This method is idempotent. It succeeds even if the terminal atomic name is not bound in the target context,
    * but throws <tt>NameNotFoundException</tt> if any of the intermediate contexts do not exist.
    * <p/>
    * <p> Any attributes associated with the name are removed. Intermediate contexts are not changed.
    *
    * @param name the name to unbind; may not be empty
    * @throws javax.naming.NameNotFoundException
    *                         if an intermediate context does not exist
    * @throws NamingException if a naming exception is encountered
    * @see #unbind(String)
    */
   public void unbind(Name name) throws NamingException {
      unbind("NAME: " + name.toString());
   }

   /**
    * Unbinds the named object. See {@link #unbind(Name)} for details.
    *
    * @param name the name to unbind; may not be empty
    * @throws javax.naming.NameNotFoundException
    *                         if an intermediate context does not exist
    * @throws NamingException if a naming exception is encountered
    */
   public void unbind(String name) throws NamingException {
      try {
         deserialize();
         bindings.remove(name);
      }
      finally {
         serialize();
      }
   }

   /**
    * Binds a new name to the object bound to an old name, and unbinds the old name.  Both names are relative to this
    * context. Any attributes associated with the old name become associated with the new name. Intermediate contexts of
    * the old name are not changed.
    *
    * @param oldName the name of the existing binding; may not be empty
    * @param newName the name of the new binding; may not be empty
    * @throws javax.naming.NameAlreadyBoundException
    *                         if <tt>newName</tt> is already bound
    * @throws NamingException if a naming exception is encountered
    * @see #rename(String,String)
    * @see #bind(Name,Object)
    * @see #rebind(Name,Object)
    */
   public void rename(Name oldName, Name newName) throws NamingException {
   }

   /**
    * Binds a new name to the object bound to an old name, and unbinds the old name. See {@link #rename(Name,Name)} for
    * details.
    *
    * @param oldName the name of the existing binding; may not be empty
    * @param newName the name of the new binding; may not be empty
    * @throws javax.naming.NameAlreadyBoundException
    *                         if <tt>newName</tt> is already bound
    * @throws NamingException if a naming exception is encountered
    */
   public void rename(String oldName, String newName) throws NamingException {
   }

   /**
    * Enumerates the names bound in the named context, along with the class names of objects bound to them. The contents
    * of any subcontexts are not included.
    * <p/>
    * <p> If a binding is added to or removed from this context, its effect on an enumeration previously returned is
    * undefined.
    *
    * @param name the name of the context to list
    * @return an enumeration of the names and class names of the bindings in this context.  Each element of the
    *         enumeration is of type <tt>NameClassPair</tt>.
    * @throws NamingException if a naming exception is encountered
    * @see #list(String)
    * @see #listBindings(Name)
    * @see javax.naming.NameClassPair
    */
   public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
      return null;
   }

   /**
    * Enumerates the names bound in the named context, along with the class names of objects bound to them. See {@link
    * #list(Name)} for details.
    *
    * @param name the name of the context to list
    * @return an enumeration of the names and class names of the bindings in this context.  Each element of the
    *         enumeration is of type <tt>NameClassPair</tt>.
    * @throws NamingException if a naming exception is encountered
    */
   public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
      return null;
   }

   /**
    * Enumerates the names bound in the named context, along with the objects bound to them. The contents of any
    * subcontexts are not included.
    * <p/>
    * <p> If a binding is added to or removed from this context, its effect on an enumeration previously returned is
    * undefined.
    *
    * @param name the name of the context to list
    * @return an enumeration of the bindings in this context. Each element of the enumeration is of type
    *         <tt>Binding</tt>.
    * @throws NamingException if a naming exception is encountered
    * @see #listBindings(String)
    * @see #list(Name)
    * @see javax.naming.Binding
    */
   public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
      return null;
   }

   /**
    * Enumerates the names bound in the named context, along with the objects bound to them. See {@link
    * #listBindings(Name)} for details.
    *
    * @param name the name of the context to list
    * @return an enumeration of the bindings in this context. Each element of the enumeration is of type
    *         <tt>Binding</tt>.
    * @throws NamingException if a naming exception is encountered
    */
   public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
      return null;
   }

   /**
    * Destroys the named context and removes it from the namespace. Any attributes associated with the name are also
    * removed. Intermediate contexts are not destroyed.
    * <p/>
    * <p> This method is idempotent. It succeeds even if the terminal atomic name is not bound in the target context,
    * but throws <tt>NameNotFoundException</tt> if any of the intermediate contexts do not exist.
    * <p/>
    * <p> In a federated naming system, a context from one naming system may be bound to a name in another.  One can
    * subsequently look up and perform operations on the foreign context using a composite name.  However, an attempt
    * destroy the context using this composite name will fail with <tt>NotContextException</tt>, because the foreign
    * context is not a "subcontext" of the context in which it is bound. Instead, use <tt>unbind()</tt> to remove the
    * binding of the foreign context.  Destroying the foreign context requires that the <tt>destroySubcontext()</tt> be
    * performed on a context from the foreign context's "native" naming system.
    *
    * @param name the name of the context to be destroyed; may not be empty
    * @throws javax.naming.NameNotFoundException
    *                         if an intermediate context does not exist
    * @throws javax.naming.NotContextException
    *                         if the name is bound but does not name a context, or does not name a context of the
    *                         appropriate type
    * @throws javax.naming.ContextNotEmptyException
    *                         if the named context is not empty
    * @throws NamingException if a naming exception is encountered
    * @see #destroySubcontext(String)
    */
   public void destroySubcontext(Name name) throws NamingException {
   }

   /**
    * Destroys the named context and removes it from the namespace. See {@link #destroySubcontext(Name)} for details.
    *
    * @param name the name of the context to be destroyed; may not be empty
    * @throws javax.naming.NameNotFoundException
    *                         if an intermediate context does not exist
    * @throws javax.naming.NotContextException
    *                         if the name is bound but does not name a context, or does not name a context of the
    *                         appropriate type
    * @throws javax.naming.ContextNotEmptyException
    *                         if the named context is not empty
    * @throws NamingException if a naming exception is encountered
    */
   public void destroySubcontext(String name) throws NamingException {
   }

   /**
    * Creates and binds a new context. Creates a new context with the given name and binds it in the target context
    * (that named by all but terminal atomic component of the name).  All intermediate contexts and the target context
    * must already exist.
    *
    * @param name the name of the context to create; may not be empty
    * @return the newly created context
    * @throws javax.naming.NameAlreadyBoundException
    *                         if name is already bound
    * @throws javax.naming.directory.InvalidAttributesException
    *                         if creation of the subcontext requires specification of mandatory attributes
    * @throws NamingException if a naming exception is encountered
    * @see #createSubcontext(String)
    * @see javax.naming.directory.DirContext#createSubcontext
    */
   public Context createSubcontext(Name name) throws NamingException {
      return null;
   }

   /**
    * Creates and binds a new context. See {@link #createSubcontext(Name)} for details.
    *
    * @param name the name of the context to create; may not be empty
    * @return the newly created context
    * @throws javax.naming.NameAlreadyBoundException
    *                         if name is already bound
    * @throws javax.naming.directory.InvalidAttributesException
    *                         if creation of the subcontext requires specification of mandatory attributes
    * @throws NamingException if a naming exception is encountered
    */
   public Context createSubcontext(String name) throws NamingException {
      return null;
   }

   /**
    * Retrieves the named object, following links except for the terminal atomic component of the name. If the object
    * bound to <tt>name</tt> is not a link, returns the object itself.
    *
    * @param name the name of the object to look up
    * @return the object bound to <tt>name</tt>, not following the terminal link (if any).
    * @throws NamingException if a naming exception is encountered
    * @see #lookupLink(String)
    */
   public Object lookupLink(Name name) throws NamingException {
      return null;
   }

   /**
    * Retrieves the named object, following links except for the terminal atomic component of the name. See {@link
    * #lookupLink(Name)} for details.
    *
    * @param name the name of the object to look up
    * @return the object bound to <tt>name</tt>, not following the terminal link (if any)
    * @throws NamingException if a naming exception is encountered
    */
   public Object lookupLink(String name) throws NamingException {
      return null;
   }

   /**
    * Retrieves the parser associated with the named context. In a federation of namespaces, different naming systems
    * will parse names differently.  This method allows an application to get a parser for parsing names into their
    * atomic components using the naming convention of a particular naming system. Within any single naming system,
    * <tt>NameParser</tt> objects returned by this method must be equal (using the <tt>equals()</tt> test).
    *
    * @param name the name of the context from which to get the parser
    * @return a name parser that can parse compound names into their atomic components
    * @throws NamingException if a naming exception is encountered
    * @see #getNameParser(String)
    * @see javax.naming.CompoundName
    */
   public NameParser getNameParser(Name name) throws NamingException {
      return null;
   }

   /**
    * Retrieves the parser associated with the named context. See {@link #getNameParser(Name)} for details.
    *
    * @param name the name of the context from which to get the parser
    * @return a name parser that can parse compound names into their atomic components
    * @throws NamingException if a naming exception is encountered
    */
   public NameParser getNameParser(String name) throws NamingException {
      return null;
   }

   /**
    * Composes the name of this context with a name relative to this context. Given a name (<code>name</code>) relative
    * to this context, and the name (<code>prefix</code>) of this context relative to one of its ancestors, this method
    * returns the composition of the two names using the syntax appropriate for the naming system(s) involved.  That is,
    * if <code>name</code> names an object relative to this context, the result is the name of the same object, but
    * relative to the ancestor context.  None of the names may be null.
    * <p/>
    * For example, if this context is named "wiz.com" relative to the initial context, then
    * <pre>
    * 	composeName("east", "wiz.com")	</pre>
    * might return <code>"east.wiz.com"</code>. If instead this context is named "org/research", then
    * <pre>
    * 	composeName("user/jane", "org/research")	</pre>
    * might return <code>"org/research/user/jane"</code> while
    * <pre>
    * 	composeName("user/jane", "research")	</pre>
    * returns <code>"research/user/jane"</code>.
    *
    * @param name   a name relative to this context
    * @param prefix the name of this context relative to one of its ancestors
    * @return the composition of <code>prefix</code> and <code>name</code>
    * @throws NamingException if a naming exception is encountered
    * @see #composeName(String,String)
    */
   public Name composeName(Name name, Name prefix) throws NamingException {
      return null;
   }

   /**
    * Composes the name of this context with a name relative to this context. See {@link #composeName(Name,Name)} for
    * details.
    *
    * @param name   a name relative to this context
    * @param prefix the name of this context relative to one of its ancestors
    * @return the composition of <code>prefix</code> and <code>name</code>
    * @throws NamingException if a naming exception is encountered
    */
   public String composeName(String name, String prefix)
         throws NamingException {
      return null;
   }

   /**
    * Adds a new environment property to the environment of this context.  If the property already exists, its value is
    * overwritten. See class description for more details on environment properties.
    *
    * @param propName the name of the environment property to add; may not be null
    * @param propVal  the value of the property to add; may not be null
    * @return the previous value of the property, or null if the property was not in the environment before
    * @throws NamingException if a naming exception is encountered
    * @see #getEnvironment()
    * @see #removeFromEnvironment(String)
    */
   public Object addToEnvironment(String propName, Object propVal)
         throws NamingException {
      return null;
   }

   /**
    * Removes an environment property from the environment of this context.  See class description for more details on
    * environment properties.
    *
    * @param propName the name of the environment property to remove; may not be null
    * @return the previous value of the property, or null if the property was not in the environment
    * @throws NamingException if a naming exception is encountered
    * @see #getEnvironment()
    * @see #addToEnvironment(String,Object)
    */
   public Object removeFromEnvironment(String propName)
         throws NamingException {
      return null;
   }

   /**
    * Retrieves the environment in effect for this context. See class description for more details on environment
    * properties.
    * <p/>
    * <p> The caller should not make any changes to the object returned: their effect on the context is undefined. The
    * environment of this context may be changed using <tt>addToEnvironment()</tt> and
    * <tt>removeFromEnvironment()</tt>.
    *
    * @return the environment of this context; never null
    * @throws NamingException if a naming exception is encountered
    * @see #addToEnvironment(String,Object)
    * @see #removeFromEnvironment(String)
    */
   public Hashtable<?, ?> getEnvironment() throws NamingException {
      return null;
   }

   /**
    * Closes this context. This method releases this context's resources immediately, instead of waiting for them to be
    * released automatically by the garbage collector.
    * <p/>
    * <p> This method is idempotent:  invoking it on a context that has already been closed has no effect.  Invoking any
    * other method on a closed context is not allowed, and results in undefined behaviour.
    *
    * @throws NamingException if a naming exception is encountered
    */
   public void close() throws NamingException {
   }

   /**
    * Retrieves the full name of this context within its own namespace.
    * <p/>
    * <p> Many naming services have a notion of a "full name" for objects in their respective namespaces.  For example,
    * an LDAP entry has a distinguished name, and a DNS record has a fully qualified name. This method allows the client
    * application to retrieve this name. The string returned by this method is not a JNDI composite name and should not
    * be passed directly to context methods. In naming systems for which the notion of full name does not make sense,
    * <tt>OperationNotSupportedException</tt> is thrown.
    *
    * @return this context's name in its own namespace; never null
    * @throws javax.naming.OperationNotSupportedException
    *                         if the naming system does not have the notion of a full name
    * @throws NamingException if a naming exception is encountered
    * @since 1.3
    */
   public String getNameInNamespace() throws NamingException {
      return null;
   }

   byte[] bytes = null;

   private void serialize() {
      if (serializing) {
         try {
            ByteArrayOutputStream bstream = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bstream);
            oos.writeObject(bindings);
            oos.close();
            bstream.close();
            bytes = bstream.toByteArray();
            bindings = null;
         }
         catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
   }

   @SuppressWarnings("unchecked")
   private void deserialize() {
      if (serializing) {
         if (bytes == null)
            bindings = new ConcurrentHashMap<String, Object>();
         else {
            try {
               ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
               bindings = (ConcurrentHashMap<String, Object>) ois.readObject();
               ois.close();
               bytes = null;
            }
            catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      }
   }
}
