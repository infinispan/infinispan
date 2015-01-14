package org.infinispan.commons.util.concurrent.jdk8backported;

import java.util.Iterator;
import java.util.NoSuchElementException;

/*
 * Written by Doug Lea and Martin Buchholz with assistance from members of
 * JCP JSR-166 Expert Group and released to the public domain, as explained
 * at http://creativecommons.org/publicdomain/zero/1.0/
 */
/**
 * This is a subset of the java.util.concurrent.ConcurrentLinkedDeque to only have
 * methods for maintaining a doubly linked list but without maintaining the 
 * java.util.Collection or java.util.Deque contracts.  This is useful to keep a linked
 * list that will never be iterated upon but instead will have references to elements
 * directly in the list that will be removed or moved as needed.
 * @author wburns
 * @since 7.1
 */
class StrippedConcurrentLinkedDeque<E> {
   /**
    * A node from which the first node on list (that is, the unique node p
    * with p.prev == null && p.next != p) can be reached in O(1) time.
    * Invariants:
    * - the first node is always O(1) reachable from head via prev links
    * - all live nodes are reachable from the first node via succ()
    * - head != null
    * - (tmp = head).next != tmp || tmp != head
    * - head is never gc-unlinked (but may be unlinked)
    * Non-invariants:
    * - head.item may or may not be null
    * - head may not be reachable from the first or last node, or from tail
    */
   transient volatile DequeNode<E> head;

   /**
    * A node from which the last node on list (that is, the unique node p
    * with p.next == null && p.prev != p) can be reached in O(1) time.
    * Invariants:
    * - the last node is always O(1) reachable from tail via next links
    * - all live nodes are reachable from the last node via pred()
    * - tail != null
    * - tail is never gc-unlinked (but may be unlinked)
    * Non-invariants:
    * - tail.item may or may not be null
    * - tail may not be reachable from the first or last node, or from head
    */
   transient volatile DequeNode<E> tail;
   
   static final DequeNode<Object> PREV_TERMINATOR, NEXT_TERMINATOR;

   final DequeNode<E> DEFAULT;

   @SuppressWarnings("unchecked")
   DequeNode<E> prevTerminator() {
       return (DequeNode<E>) PREV_TERMINATOR;
   }

   @SuppressWarnings("unchecked")
   DequeNode<E> nextTerminator() {
       return (DequeNode<E>) NEXT_TERMINATOR;
   }

   DequeNode<E> defaultNode() {
       return (DequeNode<E>) DEFAULT;
   }

   public StrippedConcurrentLinkedDeque() {
      DEFAULT = new DequeNode<>();
      head = tail = defaultNode();
   }

   static class DequeNode<E> {
       volatile DequeNode<E> prev;
       volatile E item;
       volatile DequeNode<E> next;

       DequeNode() {  // default constructor for NEXT_TERMINATOR, PREV_TERMINATOR
       }

       @Override
      public String toString() {
         return "DequeNode [item=" + item + "]";
      }

      /**
        * Constructs a new node.  Uses relaxed write because item can
        * only be seen after publication via casNext or casPrev.
        */
       DequeNode(E item) {
           UNSAFE.putObject(this, itemOffset, item);
       }

       void resetLazily(E item) {
          UNSAFE.putObject(this, itemOffset, item);
          lazySetNext(null);
          lazySetPrev(null);
       }

       boolean casItem(E cmp, E val) {
           return UNSAFE.compareAndSwapObject(this, itemOffset, cmp, val);
       }

       void lazySetNext(DequeNode<E> val) {
           UNSAFE.putOrderedObject(this, nextOffset, val);
       }

       boolean casNext(DequeNode<E> cmp, DequeNode<E> val) {
           return UNSAFE.compareAndSwapObject(this, nextOffset, cmp, val);
       }

       void lazySetPrev(DequeNode<E> val) {
           UNSAFE.putOrderedObject(this, prevOffset, val);
       }

       boolean casPrev(DequeNode<E> cmp, DequeNode<E> val) {
           return UNSAFE.compareAndSwapObject(this, prevOffset, cmp, val);
       }

       // Unsafe mechanics

       private static final sun.misc.Unsafe UNSAFE;
       private static final long prevOffset;
       private static final long itemOffset;
       private static final long nextOffset;

       static {
           try {
               UNSAFE = getUnsafe();
               Class<?> k = DequeNode.class;
               prevOffset = UNSAFE.objectFieldOffset
                   (k.getDeclaredField("prev"));
               itemOffset = UNSAFE.objectFieldOffset
                   (k.getDeclaredField("item"));
               nextOffset = UNSAFE.objectFieldOffset
                   (k.getDeclaredField("next"));
           } catch (Exception e) {
               throw new Error(e);
           }
       }
   }

   abstract class AbstractItr implements Iterator<DequeNode<E>> {
      /**
       * Next node to return item for.
       */
      private DequeNode<E> nextNode;

      /**
       * Node returned by most recent call to next. Needed by remove.
       * Reset to null if this element is deleted by a call to remove.
       */
      private DequeNode<E> lastRet;

      abstract DequeNode<E> startNode();
      abstract DequeNode<E> nextNode(DequeNode<E> p);

      AbstractItr() {
          advance();
      }

      /**
       * Sets nextNode and nextItem to next valid node, or to null
       * if no such.
       */
      private void advance() {
          lastRet = nextNode;

          DequeNode<E> p = (nextNode == null) ? startNode() : nextNode(nextNode);
          for (;; p = nextNode(p)) {
              if (p == null) {
                  // p might be active end or TERMINATOR node; both are OK
                  nextNode = null;
                  break;
              }
              E item = p.item;
              if (item != null) {
                  nextNode = p;
                  break;
              }
          }
      }

      public boolean hasNext() {
          return nextNode != null;
      }

      public DequeNode<E> next() {
          DequeNode<E> node = nextNode;
          if (node == null) throw new NoSuchElementException();
          advance();
          return node;
      }

      public void remove() {
         DequeNode<E> l = lastRet;
          if (l == null) throw new IllegalStateException();
          l.item = null;
          unlink(l);
          lastRet = null;
      }
  }

  /** Forward iterator */
  class Itr extends AbstractItr {
      DequeNode<E> startNode() { return first(); }
      DequeNode<E> nextNode(DequeNode<E> p) { return succ(p); }
  }

  /** Descending iterator */
  class DescendingItr extends AbstractItr {
     DequeNode<E> startNode() { return last(); }
     DequeNode<E> nextNode(DequeNode<E> p) { return pred(p); }
  }

   public E pollFirst() {
      for (DequeNode<E> p = first(); p != null; p = succ(p)) {
          E item = p.item;
          if (item != null && p.casItem(item, null)) {
              unlink(p);
              return item;
          }
      }
      return null;
   }

   /**
    * Returns whether a node was polled or not.  If a node was polled then the provided
    * array will contain the Node that was removed (with it's item set to null) in the 
    * 0th position of the array and the item value in the 1st position of the array.
    * @return A boolean whether or not a node was polled
    */
   public boolean pollFirstNode(Object[] nodeValue) {
      for (DequeNode<E> p = first(); p != null; p = succ(p)) {
          E item = p.item;
          if (item != null && p.casItem(item, null)) {
              unlink(p);
              nodeValue[0] = p;
              nodeValue[1] = item;
              return true;
          }
      }
      return false;
   }

   /**
    * Returns the first node, the unique node p for which:
    *     p.prev == null && p.next != p
    * The returned node may or may not be logically deleted.
    * Guarantees that head is set to the returned node.
    */
   DequeNode<E> first() {
       restartFromHead:
       for (;;)
           for (DequeNode<E> h = head, p = h, q;;) {
               if ((q = p.prev) != null &&
                   (q = (p = q).prev) != null)
                   // Check for head updates every other hop.
                   // If p == q, we are sure to follow head instead.
                   p = (h != (h = head)) ? h : q;
               else if (p == h
                        // It is possible that p is PREV_TERMINATOR,
                        // but if so, the CAS is guaranteed to fail.
                        || casHead(h, p))
                   return p;
               else
                   continue restartFromHead;
           }
   }

   /**
    * Returns the successor of p, or the first node if p.next has been
    * linked to self, which will only be true if traversing with a
    * stale pointer that is now off the list.
    */
   final DequeNode<E> succ(DequeNode<E> p) {
      // TODO: should we skip deleted nodes here?
      DequeNode<E> q = p.next;
      return (p == q) ? first() : q;
   }

   /**
    * Links e as last element.
    */
   void linkLast(DequeNode<E> newNode) {
       restartFromTail:
       for (;;)
           for (DequeNode<E> t = tail, p = t, q;;) {
               if ((q = p.next) != null &&
                   (q = (p = q).next) != null)
                   // Check for tail updates every other hop.
                   // If p == q, we are sure to follow tail instead.
                   p = (t != (t = tail)) ? t : q;
               else if (p.prev == p) // NEXT_TERMINATOR
                   continue restartFromTail;
               else {
                  // p is last node
                  newNode.lazySetPrev(p); // CAS piggyback
                  if (p.casNext(null, newNode)) {
                     // Successful CAS is the linearization point
                     // for e to become an element of this deque,
                     // and for newNode to become "live".
                     if (p != t) // hop two nodes at a time
                        casTail(t, newNode);  // Failure is OK.
                     return;
                  }
                  // Lost CAS race to another thread; re-read next
               }
           }
   }
   private static final int HOPS = 2;

   /**
    * Unlinks non-null node x.
    * @return whether this node can be reused - A node may not be reused if it was a head
    * or tail because it can still have valid references to it
    */
   void unlink(DequeNode<E> x) {
       // assert x != null;
       // assert x.item == null;
       // assert x != PREV_TERMINATOR;
       // assert x != NEXT_TERMINATOR;

       final DequeNode<E> prev = x.prev;
       final DequeNode<E> next = x.next;
       if (prev == null) {
           unlinkFirst(x, next);
           return;
       } else if (next == null) {
           unlinkLast(x, prev);
           return;
       } else {
           // Unlink interior node.
           //
           // This is the common case, since a series of polls at the
           // same end will be "interior" removes, except perhaps for
           // the first one, since end nodes cannot be unlinked.
           //
           // At any time, all active nodes are mutually reachable by
           // following a sequence of either next or prev pointers.
           //
           // Our strategy is to find the unique active predecessor
           // and successor of x.  Try to fix up their links so that
           // they point to each other, leaving x unreachable from
           // active nodes.  If successful, and if x has no live
           // predecessor/successor, we additionally try to gc-unlink,
           // leaving active nodes unreachable from x, by rechecking
           // that the status of predecessor and successor are
           // unchanged and ensuring that x is not reachable from
           // tail/head, before setting x's prev/next links to their
           // logical approximate replacements, self/TERMINATOR.
           DequeNode<E> activePred, activeSucc;
           boolean isFirst, isLast;
           int hops = 1;

           // Find active predecessor
           for (DequeNode<E> p = prev; ; ++hops) {
               if (p.item != null) {
                   activePred = p;
                   isFirst = false;
                   break;
               }
               DequeNode<E> q = p.prev;
               if (q == null) {
                   if (p.next == p)
                       return;
                   activePred = p;
                   isFirst = true;
                   break;
               }
               else if (p == q)
                  return;
               else
                   p = q;
           }

           // Find active successor
           for (DequeNode<E> p = next; ; ++hops) {
               if (p.item != null) {
                   activeSucc = p;
                   isLast = false;
                   break;
               }
               DequeNode<E> q = p.next;
               if (q == null) {
                   if (p.prev == p)
                       return;
                   activeSucc = p;
                   isLast = true;
                   break;
               }
               else if (p == q)
                   return;
               else
                   p = q;
           }

           // TODO: better HOP heuristics
           if (hops < HOPS
               // always squeeze out interior deleted nodes
               && (isFirst | isLast))
               return;

           // Squeeze out deleted nodes between activePred and
           // activeSucc, including x.
           skipDeletedSuccessors(activePred);
           skipDeletedPredecessors(activeSucc);

           // Try to gc-unlink, if possible
           if ((isFirst | isLast) &&

               // Recheck expected state of predecessor and successor
               (activePred.next == activeSucc) &&
               (activeSucc.prev == activePred) &&
               (isFirst ? activePred.prev == null : activePred.item != null) &&
               (isLast  ? activeSucc.next == null : activeSucc.item != null)) {

               updateHead(); // Ensure x is not reachable from head
               updateTail(); // Ensure x is not reachable from tail

               // Finally, actually gc-unlink
               x.lazySetPrev(isFirst ? prevTerminator() : x);
               x.lazySetNext(isLast  ? nextTerminator() : x);
               return;
           }
           return;
       }
   }

   /**
    * Unlinks non-null first node.
    */
   private void unlinkFirst(DequeNode<E> first, DequeNode<E> next) {
       // assert first != null;
       // assert next != null;
       // assert first.item == null;
       for (DequeNode<E> o = null, p = next, q;;) {
           if (p.item != null || (q = p.next) == null) {
               if (o != null && p.prev != p && first.casNext(next, p)) {
                   skipDeletedPredecessors(p);
                   if (first.prev == null &&
                       (p.next == null || p.item != null) &&
                       p.prev == first) {

                       updateHead(); // Ensure o is not reachable from head
                       updateTail(); // Ensure o is not reachable from tail

                       // Finally, actually gc-unlink
                       o.lazySetNext(o);
                       o.lazySetPrev(prevTerminator());
                   }
               }
               return;
           }
           else if (p == q)
               return;
           else {
               o = p;
               p = q;
           }
       }
   }

   /**
    * Unlinks non-null last node.
    */
   private void unlinkLast(DequeNode<E> last, DequeNode<E> prev) {
       // assert last != null;
       // assert prev != null;
       // assert last.item == null;
       for (DequeNode<E> o = null, p = prev, q;;) {
           if (p.item != null || (q = p.prev) == null) {
               if (o != null && p.next != p && last.casPrev(prev, p)) {
                   skipDeletedSuccessors(p);
                   if (last.next == null &&
                       (p.prev == null || p.item != null) &&
                       p.next == last) {

                       updateHead(); // Ensure o is not reachable from head
                       updateTail(); // Ensure o is not reachable from tail

                       // Finally, actually gc-unlink
                       o.lazySetPrev(o);
                       o.lazySetNext(nextTerminator());
                   }
               }
               return;
           }
           else if (p == q)
               return;
           else {
               o = p;
               p = q;
           }
       }
   }

   /**
    * Guarantees that any node which was unlinked before a call to
    * this method will be unreachable from head after it returns.
    * Does not guarantee to eliminate slack, only that head will
    * point to a node that was active while this method was running.
    */
   private final void updateHead() {
       // Either head already points to an active node, or we keep
       // trying to cas it to the first node until it does.
       DequeNode<E> h, p, q;
       restartFromHead:
       while ((h = head).item == null && (p = h.prev) != null) {
           for (;;) {
               if ((q = p.prev) == null ||
                   (q = (p = q).prev) == null) {
                   // It is possible that p is PREV_TERMINATOR,
                   // but if so, the CAS is guaranteed to fail.
                   if (casHead(h, p))
                       return;
                   else
                       continue restartFromHead;
               }
               else if (h != head)
                   continue restartFromHead;
               else
                   p = q;
           }
       }
   }

   /**
    * Guarantees that any node which was unlinked before a call to
    * this method will be unreachable from tail after it returns.
    * Does not guarantee to eliminate slack, only that tail will
    * point to a node that was active while this method was running.
    */
   private final void updateTail() {
       // Either tail already points to an active node, or we keep
       // trying to cas it to the last node until it does.
       DequeNode<E> t, p, q;
       restartFromTail:
       while ((t = tail).item == null && (p = t.next) != null) {
           for (;;) {
               if ((q = p.next) == null ||
                   (q = (p = q).next) == null) {
                   // It is possible that p is NEXT_TERMINATOR,
                   // but if so, the CAS is guaranteed to fail.
                   if (casTail(t, p))
                       return;
                   else
                       continue restartFromTail;
               }
               else if (t != tail)
                   continue restartFromTail;
               else
                   p = q;
           }
       }
   }

   void skipDeletedPredecessors(DequeNode<E> x) {
       whileActive:
       do {
           DequeNode<E> prev = x.prev;
           // assert prev != null;
           // assert x != NEXT_TERMINATOR;
           // assert x != PREV_TERMINATOR;
           DequeNode<E> p = prev;
           findActive:
           for (;;) {
               if (p.item != null)
                   break findActive;
               DequeNode<E> q = p.prev;
               if (q == null) {
                   if (p.next == p)
                       continue whileActive;
                   break findActive;
               }
               else if (p == q)
                   continue whileActive;
               else
                   p = q;
           }

           // found active CAS target
           if (prev == p || x.casPrev(prev, p))
               return;

       } while (x.item != null || x.next == null);
   }

   void skipDeletedSuccessors(DequeNode<E> x) {
       whileActive:
       do {
           DequeNode<E> next = x.next;
           // assert next != null;
           // assert x != NEXT_TERMINATOR;
           // assert x != PREV_TERMINATOR;
           DequeNode<E> p = next;
           findActive:
           for (;;) {
               if (p.item != null)
                   break findActive;
               DequeNode<E> q = p.next;
               if (q == null) {
                   if (p.prev == p)
                       continue whileActive;
                   break findActive;
               }
               else if (p == q)
                   continue whileActive;
               else
                   p = q;
           }

           // found active CAS target
           if (next == p || x.casNext(next, p))
               return;

       } while (x.item != null || x.prev == null);
   }

   /**
    * Returns the predecessor of p, or the last node if p.prev has been
    * linked to self, which will only be true if traversing with a
    * stale pointer that is now off the list.
    */
   final DequeNode<E> pred(DequeNode<E> p) {
       DequeNode<E> q = p.prev;
       return (p == q) ? last() : q;
   }

   public DequeNode<E> peekFirstNode() {
      for (DequeNode<E> p = first(); p != null; p = succ(p)) {
          E item = p.item;
          if (item != null)
              return p;
      }
      return null;
  }

   public DequeNode<E> peekLastNode() {
      for (DequeNode<E> p = last(); p != null; p = pred(p)) {
          E item = p.item;
          if (item != null)
              return p;
      }
      return null;
  }

   /**
    * Returns the last node, the unique node p for which:
    *     p.next == null && p.prev != p
    * The returned node may or may not be logically deleted.
    * Guarantees that tail is set to the returned node.
    */
   DequeNode<E> last() {
       restartFromTail:
       for (;;)
           for (DequeNode<E> t = tail, p = t, q;;) {
               if ((q = p.next) != null &&
                   (q = (p = q).next) != null)
                   // Check for tail updates every other hop.
                   // If p == q, we are sure to follow tail instead.
                   p = (t != (t = tail)) ? t : q;
               else if (p == t
                        // It is possible that p is NEXT_TERMINATOR,
                        // but if so, the CAS is guaranteed to fail.
                        || casTail(t, p))
                   return p;
               else
                   continue restartFromTail;
           }
   }

   private boolean casHead(DequeNode<E> cmp, DequeNode<E> val) {
      return UNSAFE.compareAndSwapObject(this, headOffset, cmp, val);
  }

  private boolean casTail(DequeNode<E> cmp, DequeNode<E> val) {
      return UNSAFE.compareAndSwapObject(this, tailOffset, cmp, val);
  }

  // Unsafe mechanics

  private static final sun.misc.Unsafe UNSAFE;
  private static final long headOffset;
  private static final long tailOffset;
  static {
      PREV_TERMINATOR = new DequeNode<Object>();
      PREV_TERMINATOR.next = PREV_TERMINATOR;
      NEXT_TERMINATOR = new DequeNode<Object>();
      NEXT_TERMINATOR.prev = NEXT_TERMINATOR;
      try {
          UNSAFE = getUnsafe();
          Class<?> k = StrippedConcurrentLinkedDeque.class;
          headOffset = UNSAFE.objectFieldOffset
              (k.getDeclaredField("head"));
          tailOffset = UNSAFE.objectFieldOffset
              (k.getDeclaredField("tail"));
      } catch (Exception e) {
          throw new Error(e);
      }
  }

  static sun.misc.Unsafe getUnsafe() {
     try {
        return sun.misc.Unsafe.getUnsafe();
     } catch (SecurityException tryReflectionInstead) {}
     try {
        return java.security.AccessController.doPrivileged
              (new java.security.PrivilegedExceptionAction<sun.misc.Unsafe>() {
                 public sun.misc.Unsafe run() throws Exception {
                    Class<sun.misc.Unsafe> k = sun.misc.Unsafe.class;
                    for (java.lang.reflect.Field f : k.getDeclaredFields()) {
                       f.setAccessible(true);
                       Object x = f.get(null);
                       if (k.isInstance(x))
                          return k.cast(x);
                    }
                    throw new NoSuchFieldError("the Unsafe");
                 }});
     } catch (java.security.PrivilegedActionException e) {
        throw new RuntimeException("Could not initialize intrinsics",
              e.getCause());
     }
  }
}