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
package org.infinispan.spring.provider.sample;

/**
 * <p>
 * A simple, woefully incomplete {@code DAO} for storing, retrieving and removing {@link Book
 * <code>Books</code>}.
 * </p>
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @since 5.1
 */
public interface BookDao {

   /**
    * <p>
    * Look up and return the {@code Book} identified by the supplied {@code bookId}, or {@code null}
    * if no such book exists.
    * </p>
    * 
    * @param bookId
    * @return The {@code Book} identified by the supplied {@code bookId}, or {@code null}
    */
   Book findBook(Integer bookId);

   /**
    * <p>
    * Remove the {@code Book} identified by the supplied {@code bookId} from this store.
    * </p>
    * 
    * @param bookId
    */
   void deleteBook(Integer bookId);

   /**
    * <p>
    * Store the provided {@code book}. Depending on whether {@code book} has already been store
    * before this method will either perform an {@code insert} or an {@code update}. Return the
    * stored book.
    * </p>
    * 
    * @param book
    *           The book to store
    * @return The stored book
    */
   Book storeBook(Book book);
}
