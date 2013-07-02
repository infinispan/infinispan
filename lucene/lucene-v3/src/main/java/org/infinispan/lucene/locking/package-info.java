/**
 * Lucene's IndexWriter instances are threadsafe but you can have only one open on the index, so when opening an IndexWriter an
 * index-wide lock needs to be acquired. When using Infinispan this lock needs reliable distribution, so two implementations
 * are provided which where tested with the Infinispan Directory and are suited for distributed locking, but you could provide your own implementation of LockFactory.
 * You might also disable the locking altogether if you have application level or other external guarantees that no two IndexWriters
 * will ever be opened.
 */
package org.infinispan.lucene.locking;
