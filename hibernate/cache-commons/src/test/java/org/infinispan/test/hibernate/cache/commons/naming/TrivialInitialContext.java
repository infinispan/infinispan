package org.infinispan.test.hibernate.cache.commons.naming;

import java.util.Hashtable;
import java.util.concurrent.ConcurrentMap;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;

public class TrivialInitialContext implements Context {

   private final ConcurrentMap<String, Object> namedObjects;

   public TrivialInitialContext(ConcurrentMap<String, Object> namedObjects) {
      this.namedObjects = namedObjects;
   }

   @Override
   public Object lookup(Name name) {
      return lookup(name.toString());
   }

   @Override
   public Object lookup(String name) {
      return namedObjects.get(name);
   }

   @Override
   public void bind(Name name, Object obj) throws NamingException {
      bind(name.toString(), obj);
   }

   @Override
   public void bind(String name, Object obj) throws NamingException {
      if (namedObjects.putIfAbsent(name, obj) != null) {
         throw new IllegalStateException("Name already bound: " + name);
      }
   }

   @Override
   public void rebind(Name name, Object obj) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void rebind(String name, Object obj) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void unbind(Name name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void unbind(String name) {
      namedObjects.remove(name);
   }

   @Override
   public void rename(Name oldName, Name newName) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void rename(String oldName, String newName) {
      throw new UnsupportedOperationException();
   }

   @Override
   public NamingEnumeration<NameClassPair> list(Name name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public NamingEnumeration<NameClassPair> list(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public NamingEnumeration<Binding> listBindings(Name name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public NamingEnumeration<Binding> listBindings(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void destroySubcontext(Name name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void destroySubcontext(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Context createSubcontext(Name name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Context createSubcontext(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object lookupLink(Name name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object lookupLink(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public NameParser getNameParser(Name name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public NameParser getNameParser(String name) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Name composeName(Name name, Name prefix) {
      throw new UnsupportedOperationException();
   }

   @Override
   public String composeName(String name, String prefix) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object addToEnvironment(String propName, Object propVal) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object removeFromEnvironment(String propName) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Hashtable<?, ?> getEnvironment() {
      return null;
   }

   @Override
   public void close() {
      // Do nothing
   }

   @Override
   public String getNameInNamespace() {
      throw new UnsupportedOperationException();
   }
}
