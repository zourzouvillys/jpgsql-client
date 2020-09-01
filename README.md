# Java PostgreSQL Client

Non JDBC client for working with PostgreSQL.

It provides a lean & simple API without all the JDBC bells and whistles (overhead).  The API can be used for both blocking and non blocking clients, and a default JDBC thread pooled backed version lets you use it in the non blocking fashion.


## Dependency Updates

Althoguh specific versions are ferenced in dependncies, using a lockfile ensures we are aware of exactly what is being pulled in.

```
./gradlew dependencies --write-locks
```

