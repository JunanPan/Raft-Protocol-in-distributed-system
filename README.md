# A Raft protocol implemented by Go.


## Starter Code

The starter code for this project is organized as follows:

```

src/github.com/cmu440/        
  raft/                            Raft implementation, tests and test helpers

  rpc/                             RPC library that must be used for implementing Raft

```


### Executing the tests

To run the checkpoint tests, run the following from the `src/github.com/cmu440/raft/` folder

```sh
go test -run 2A
```

Check code for race conditions using Go’s race detector:

```sh
go test -race -run 2A
```


To execute all the tests, run the following from the `src/github.com/cmu440/raft/` folder

```sh
go test
```

Check code for race conditions using Go’s race detector:

```sh
go test -race
```
