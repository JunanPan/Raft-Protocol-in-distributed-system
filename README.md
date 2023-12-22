# RAFT PROTOCOL IMPLEMENTED BY GO


## Starter Code

The starter code for this project is organized as follows:

```

src/github.com/cmu440/        
  raft/                            Raft implementation, tests and test helpers

  rpc/                             RPC library that must be used for implementing Raft

```

## Instructions


### Executing the official tests

#### 1. Checkpoint

To run the checkpoint tests, run the following from the `src/github.com/cmu440/raft/` folder

```sh
go test -run 2A
```

We will also check your code for race conditions using Go’s race detector:

```sh
go test -race -run 2A
```

#### 2. Full test

To execute all the tests, run the following from the `src/github.com/cmu440/raft/` folder

```sh
go test
```

We will also check your code for race conditions using Go’s race detector:

```sh
go test -race
```


## Miscellaneous

### Reading the API Documentation

Before you begin the project, you should read and understand all of the starter code we provide.
To make this experience a little less traumatic (we know, it's a lot :P),
fire up a web server and read the documentation in a browser by executing the following commands:

1. Install `godoc` globally, by running the following command **outside** the `src/github.com/cmu440` directory:
```sh
go install golang.org/x/tools/cmd/godoc@latest
```
2. Start a godoc server by running the following command **inside** the `src/github.com/cmu440` directory:
```sh
godoc -http=:6060
```
3. While the server is running, navigate to [localhost:6060/pkg/github.com/cmu440](http://localhost:6060/pkg/github.com/cmu440) in a browser.
