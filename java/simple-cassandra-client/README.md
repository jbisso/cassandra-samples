cassandra-simple-client
=======================

Some sample clients from the Java driver documentation

Build environment
-----------------

* JDK 7 (should work OK with 1.6)
* Maven 3.0.x
* Eclipse 4.2.x
* m2eclipse 1.4 (Maven Eclipse plug-in)

You will need to be running a Cassandra cluster that you can connect to. 
You should be running Cassandra 1.2.1 or later. The nodes must be configured
to use the CQL3 binary protocol (in the `cassandra.yaml` file):

start_native_transport: true

You might have to fiddle with the project settings in Eclipse.

Links
-----

* https://github.com/datastax/java-driver
* https://github.com/pcmanus/ccm
