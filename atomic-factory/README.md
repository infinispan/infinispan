# Atomic Object Factory module

The class org.infinispan.AtomicObjectFactory implements a factory of atomic objects.
This factory is universal in the sense that it can instantiate an object of any (serializable) class
atop an Infinispan cache, making transparently the object replicated and durable,
while ensuring strong consistency despite concurrent access.

## Basic Usage

Using the AtomicObjectFactory is fairly simple. Below, we illustrate a base use case.

```
AtomicObjectFactory factory = new AtomicObjectFactory(c1); // c1 is both synchronous and transactional
Set set = (Set) factory.getInstanceOf(HashSet.class, "k"); // k is the key to store set inside c1
set.add("something"); // some call examples
System.out.println(set.toString())
set.addAll(set);
factory.disposeInstanceOf(HashSet.class, "set", true); // to store in a persistent way the object
```

Additional examples are provided in org.infinispan.AtomicObjectFactoryTest.

## Limitations

The implementation requires that all the arguments of the methods of the object are Serializable,
as well as the object itself.

## Going Further

### White Paper

The factory is described in Section 4 of the paper untitled "On the Support of
Versioning in Distributed Key-Value Stores" published at the 33rd IEEE Symposium on Reliable Distributed Systems
(SRDS'14). A preprint version of this paper is available at the following [address]
(https://drive.google.com/open?id=0BwFkGepvBDQoTEdPS0x6VXhqMW8&authuser=0).

### High-level Implementation Details

We built the factory on top of the transactional facility of Infinispan.
In more details, when the object is created, we store both a local copy and a proxy registered as a cache listener.
We serialize every call in a transaction consisting of a single put operation.
When the call is de-serialized its applied to the local copy and, in case the calling process was local,
the tentative call value is returned (this mechanism is implemented as a future object).
