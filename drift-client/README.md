NOTES:

* Review the ClientEventInterface
* transport and protocol should be configurable 

# Drift Client

Drift Client is a simple library annotating Thrift clients.

# Clients

In Drift, a Thrift client is simply a Java interface annotated with `@ThriftService`.  In 
addition to annotating the interface directly, Drift supports annotations on a super 
interfaces..  For example, the following describes a scribe client:
```java
    @ThriftService
    public interface Scribe
    {
        @ThriftMethod
        ResultCode log(List<LogEntry> messages);
    }
```
To make the client method asynchronous, simpleychange the return type of the
corresponding method to a ListenableFuture, as shown here:
```java
    @ThriftService
    public interface Scribe
    {
        @ThriftMethod()
        ListenableFuture<List<Integer>> getValues();
    }
```
and the future will be set when the server returns a value. When adding listeners that do
non-trivial work when the future is set, keep in mind that if you do not provide an
executor for listeners to run on, they will run on the NIO threads, and therefore can
potentially block other clients.


# Parameters

In Thrift, method parameters are encoded as a Thrift struct, so each parameter
must have a name and an id.  Drift assumes the parameters are numbered starting
with one, and assuming you have compiled with debug symbols, the Thrift
parameter names match the Java parameter names.  If you want to use a different
id or name, simply annotate the parameter as follows:
 ```java
    @ThriftService
    public interface Scribe
    {
        @ThriftMethod
        ResultCode log(@ThriftField(value = 3, name = "mesg") List<LogEntry> messages);
    }
```

# Exceptions

As with method parameters, Thrift encodes the response as a struct with field
zero being a standard return and exceptions be stored in higher number fields.
If the Java method throws only one exception annotated with @ThriftStruct,
Drift will assume the result struct field id is `1`.  Otherwise you will need to
add the extremely verbose `@ThriftException` annotations as follows:
```java
    @ThriftMethod(exception = {
          @ThriftException(type = MyException.class, id = 1),
          @ThriftException(type = MyOther.class, id = 2)
    })
    void doSomething() throws MyException, MyOther;
```

# Static Drift Client Factory

Drift client implementations can be created using the `DriftClientFactory`.  The
following code creates a Scribe client:
```java
        ThriftCodecManager codecManager = new ThriftCodecManager();
        AddressSelector addressSelector = new SimpleAddressSelector(scribeHostAddreses);
        DriftNettyClientConfig config = new DriftyNettyClientConfig()
                .setPoolEnabled(true);
        try (DriftNettyMethodInvokerFactory<?> methodInvokerFactory = createStaticDriftNettyMethodInvokerFactory(config)) {
            DriftClientFactory proxyFactory = new DriftClientFactory(codecManager, methodInvokerFactory, addressSelector);

            Scribe scribe = proxyFactory.createDriftClient(Scribe.class).get();
            
            // use client
        }
```

As you can see, the construction of a `DriftClientFactory` requires a few supporting 
services, which are described below.
  
## ThriftCodecManager

A `ThriftCodecManager` caches the description of every Thrift type used by the clients.  Extracting
service metadata is expensive, so a single `ThriftCodecManager` should be used to create
all the clients you will need.

## AddressSelector

An `AddressSelector` select host addresses that are running the ThriftServer.  Typically, 
this service tracks all hosts running the service, and selects a random subset to try for an 
invocation.  The `AddressSelector` is also notified of addresses that failed to connect, so 
it can perform simple tracking of host state.

## MethodInvokerFactory

A `MethodInvokerFactory` is handles connection and worker thread pools. Connections and
threads are expensive resources, so a single `MethodInvokerFactory` should be used to create
all the clients you will need.  Since the `MethodInvokerFactory` is maintaining pools, a `close`
method is typically provided to shutdown the pools.

There are currently two transport implementations that `MethodInvokerFactory`.  Drift Netty, which
is used in the example above, is provides `DriftNettyMethodInvokerFactory`, and Apache Thrift provides
`ApacheThriftMethodInvokerFactory`.  Each transport requires sightly different configuration, 
so each transport provides a different configuration class.

# Guice Support

Drift includes optional support for binding clients into Guice.

## Client Binding

To bind a client, add the `ThriftClientModule` and a transport implementation module (e.g.
`DriftNettyClientModule` of `ApacheThriftClientModule`), and bind the clients with the fluent 
`DriftClientBinder` as follows:
```java

    Injector injector = Guice.createInjector(Stage.PRODUCTION,
            new ConfigurationModule(new ConfigurationFactory(ImmutableMap.<String, String>of())),
            new ThriftCodecModule(),
            new DriftyNettyClientModule(),
            new Module()
            {
                @Override
                public void configure(Binder binder)
                {
                    driftClientBinder(binder).bindDriftClient(Scribe.class);
                }
            });
```

Then, Guice can inject a Thrift client implementation.  For example:
```java
    @Inject
    public MyClass(Scribe scribeClient)
    {
        scribeClient.log(entries);
    }
```
