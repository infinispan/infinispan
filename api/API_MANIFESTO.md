# The API Manifesto

# Infinispan API dependency module

The ```infinispan-api``` dependency will contain the public API. This module will be independent from the other modules. 
Dependency with implementations will be **logical**. This will keep the modularity needed for the different use cases.

Examples:
 * Cache use case
 
 * NoSQL data store use case
 
 * Tasks, concurrency, counters uses cases

The dependencies to thirty-party libraries must be avoided. This module should contain dependencies to JDK classes, 
not more.

If you really **need** for a reason to use an external library to be part of an API. 
Here is a non-exhaustive list of what we should discuss before promoting an API that uses an external dependency:

-Is there a way to keep that library in implementation instead of public API? Creating our Infinispan 
public classes and keeping it internal
- Are we are being too opinionated?
- Who are we depending on? What is going to happen when this library won't be maintained or will break?
- Is this library such important that is considered to be included in the JDK in the near future?

**The promotion to public official API should be done by care and debate. 
By default we don't use other classes than JDK + ours.**

# User First

* Before making an API public, consider to test the usability with someone not familiar with the feature

* Convention over Configuration: Make sure a default behaviour exists. Don't make me think.

* Avoid exposing internals as much as possible. Even if your feature lays on top of a cache today, internals can change.

* Each public API/Feature must have a simple tutorial showcasing the example. Without a demo, the API does not exist

* Focus on use cases over features. Try to showcase in a demo, blog post and tutorial how to solve a problem they have

* Ask for user's feedback actively (zulip, user forums, blog post, twitter, github ...)

* Be clear about whats PUBLIC API and what internals/implementations are. Place your implementations under `impl.*` package.

# Documentation

* Public API must have Java Doc

* Public API must have User Doc 
  
* Public API must have a Simple Tutorial (written + code)   

* Public API must have at least 1 blog post linking to the How-To tutorial


# Experimental API

Promotion to public official API should be done by care.

* If you are unsure about an API because it's very very experimental, mark it as ```@Experimental``` and don't put it in the public API module.
 If the API is indeed 

* Don't let ```@Experimental``` tag last forever. After a while (1/2 releases) and user driven, promote the API to the 
public module. 

# Client/Server First

* New interfaces should be the same for embedded and client/server

* Always start defining your API considering users in client/server mode. Embedded implementation will follow.

* Always start defining your methods for non blocking or async. Sync implementation will follow.

* If a particular need is demanded for embedded, before doing something specific
   - Discuss with the team the need. Is it really mandatory?
   - Is this a general purpose or a really advanced need? 
   - Will the functionality be widely used? Why is this embedded only then?
   
Embedded only API should be an exception at this point and they might not live in the **infinispan-api** module.


# When do we create a new module/dependency?

This has to be discussed case by case with the team. Several factors need to be considered.

* Can the implementation of a new API live in an already existing module?

* Would be better for the user if we create a new module, but we add the needed dependencies for the user?


# Creating API

* When you create a new API, you should consider the use case it's about to solve. 

* Avoid Swiss knifes on interfaces. Keep it focuses and small enough.

* Remember public APIs are like diamonds, they last forever. Rarely we will be allowed to just remove an API without 
breaking compatibility. 


# Evolving API

* Adding a new method to an existing interface must be done with care. Consider the need of the method versus creating a
new interface.

* If there is no other choice that adding a new method, provide a **default implementation**

* Methods tend to need parameter overloading overtime. Use a **Parameters** object to assure evolution over time instead 
of a list of parameters

* Versioning: if a new version of the API has to be created, consider adding `v1`, `v2` in the packaging


# Deprecating API

* Deprecating a public method is sad, but it happens. Provide the proper documentation and the reasons for the deprecation.
Add the alternative code explaining the changes. 






 
