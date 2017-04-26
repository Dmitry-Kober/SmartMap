# SmartMap

// For now, the component is in a very DRAFT MVP stage!!!

SmartMap is a key-value file-based reliable storage, where reliabily means fault tolerance.

The insight of the implementation is that there are two layers: register and file system.

The register is a dynamic persistent storage for all key-value manipulations, organized having the 'write ahead log' idea in mind. 
It is implemented based on the SQLite embedded database. The very important point here is that due to the SQLite specifics, 
all its transactions are of the SERIALIZABLE isolation level.

The file system layer is implemented basing on two simple file manipulations: CREATE and DELETE.  
As you can see, there is no UPDATE operation ever performed. Each value is stored in a separate single IMMUTABLE file.
Even if there are two requests to put different values for the same key, two files will be created (but with different timestamps).
When a reader comes, it takes only the latest version of the value from the register and treats it as a final value.

In this implementation there are a lot of files created and a lot of records are added to the database register.
In order to reduce a number of entities created and manipulated, there is a dedicated GC/housekeeper thread that is triggered 
each second (default value) and identifys and removes all unused ones (in both the register and the file system).

In order to provide scalability, there is a notion of a shard. If required several shards could be configured and the HashBasedRequestManager
class will reliably distribute all the key-value pair between the shards. For now, the default affinity function is based on a key's has code.
