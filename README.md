# pgflow: Orchestrate postgres commands through a dependency framework.

## Features:
* Run batched commands in background through pgasync library
* Organize tasks into a dependency framework for automatic start or 
  cancellation of work
* No dependencies outside of pgasync (a SQL extension) or dblink
* Output flow diagrams in graphviz 
* Simple configuration and usage

## Concepts
* pgflow defines the following concepts:
### Flow
A flow is a sequences set of processes to run in a DAG.  Those proceses are
always implemented in stored procedures, but are allowed to do anything a stored
procedure an do. Those procedures can run on the pgflow server itself, but can
also be run on any server the pgflow server can reach with dblink.

### Node
### Step
### Callback Routine

## Installation

### Installation of pgasync library

### Installation of pgflow library

```shell
psql < pgflow.sql # install first, for client and server
psql < pgflow_server.sql # install second, for server only
psql < pgflow_ui.sql # install third, for server only
```

### Configuration of pgflow and pgasync

### loading pgflow library 

## Important data strucutres

### Tables

#### arguments

The client only table flow.arguments contains a cache of the flow arguments so
that they don't have to be passed repeatedly to the processing routines.

### Views
#### v_flow_task
#### v_flow_node_status
#### v_flow_status

### Composite Types
#### callback_arguments_t

pgflow (unlike the async library) does not execute arbitrary queries but only 
runs stored procedure with a specfic argument of type callback_arguments_t.

The purpose fo these arguments is to identify the processing context so that 
the procedure can have the information it needs to complete work.

```sql

CREATE TYPE flow.callback_arguments_t AS
(
  flow_id BIGINT,         /* as created by run_flow() */
  flow TEXT,              /* as created by configure_flow() */
  flow_arguments JSONB,   /* as passed to run_flow() */
  node TEXT,              /* this node of processing chain */
  step_arguments JSONB,   /* arguments particular to this step of the node */
  task_id BIGINT          /* async task id */
);

```


## API

### configure_flow()

Configuring a flow

```sql
CREATE OR REPLACE FUNCTION flow.configure_flow(
  _flow_name TEXT,
  _configuration JSONB,
  _append BOOL DEFAULT FALSE) RETURNS VOID AS
$$
...
```

### run_flow() 
Run_flow runs a previously configured flow.  A flow can be run many times. 

```sql
CREATE OR REPLACE FUNCTION flow.run_flow(
  _flow TEXT,
  _arguments JSONB,
  _only_these_nodes TEXT[] DEFAULT NULL, 
  _add_parents BOOL DEFAULT FALSE, /* XXX: not implemented */
  _add_children BOOL DEFAULT FALSE, /* XXX: not implemented */
  _parent_task_id BIGINT DEFAULT NULL,
  flow_id OUT BIGINT) RETURNS BIGINT AS
...
```

### push_steps()

```sql
CREATE OR REPLACE PROCEDURE flow.push_steps(
  _flow_id BIGINT,  
  _node TEXT,
  _arguments JSONB[],
  _flush_transaction_when_client BOOL DEFAULT TRUE) AS
$$
...
```

### finish()

```sql
CREATE OR REPLACE PROCEDURE flow.finish(
  _args flow.callback_arguments_t,
  _failed BOOL DEFAULT false,
  _error_message TEXT DEFAULT NULL,
  _flush_transaction_when_client BOOL DEFAULT true) AS
$$
...
```

### cancel()


```sql
CREATE OR REPLACE FUNCTION flow.cancel(
  _flow_id BIGINT) RETURNS VOID AS
$$
...
```

## Examples


## FAQ: 
  Q: Will pgflow ever invoke non-procedure targts?  What about running an http
     request?

  A: It is very unlikely pgflow will ever suport invoking non-procedure targets,
     this would make the library more complex and require additional 
     dependencies.  However, the procedures invoked from pgflow can take just 
     about any action that is supported by the rich postgres extension 
     ecosystem.

  Q: What's the difference between nodes and steps?

  A: Nodes are appropriate to organize dependencies. Steps are appropriate to 
     process lists of things that operate at the same dependency level. 

  Q: What is the minimum version of postgres async can run on?

  A: pgflow (and async) require postgres 11, which introduced stored procedure 
     support

  Q: Does pgflow include an administrative interface?  

  A: Currently no, as an interface would require a lot of additional 
     considerations such as authorization, authentication, and other complex 
     requirements.  However,  the library is defined to have an interface bolted 
     directly on. 

  Q: What kinds of considerations are there for invoking endpoints with 3rd 
     party libraries that can run a very long time?

  A: pgflow through pgasync runs all queries in the background.  However, it's 
     important to understand that a long running procedure that calls into 
     another library (say, though pgsql-http) will tie down an pgasync 
     connction for as long as that external routines is running. Furthermore,
     in may cases, many extensions and other 3rd party libraries do not 
     respond appropriately to cancel.  It may be better then to have the 
     target be configured asynchronously so that reponse is not waited for,
     rather, the external routine response back when done. 

  Q: Can AWS lambda functions be invoked?

  A: Yes! AWS lambda funcions can be invoked synchronously or asynchronously.     



  

