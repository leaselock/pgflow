
/* Implements client side interfaces and and stanard structures for flow 
 * library. 
*/


\if :bootstrap

CREATE SCHEMA flow;

/* 
 * flow arguments are cached client side.
 */
CREATE TABLE flow.arguments
(
  flow_id BIGINT PRIMARY KEY,
  arguments JSONB
);

CREATE TYPE flow.callback_arguments_t AS
(
  flow_id BIGINT,
  flow TEXT,
  flow_arguments JSONB,
  node TEXT,
  step_arguments JSONB,
  task_id BIGINT
);



\endif




/* 
 * Some nodes initialize steps on the fly.  To avoid race conditions they have 
 * to be pushed as any other task...the steps much be confirmed before the node
 * resolves.
 *
 */
CREATE OR REPLACE FUNCTION flow.push_steps(
  _flow_id BIGINT,  
  _node TEXT,
  _arguments JSONB[]) RETURNS VOID AS
$$
DECLARE
  _held_steps JSONB;
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    PERFORM * FROM dblink(
      async.server(), 
      format(
        'SELECT 0 FROM flow.push_steps(%s, %s, %s)',
        quote_literal($1),
        quote_literal($2),
        quote_literal($3))) AS R(V int);

    RETURN;
  END IF; 

  PERFORM flow.push_tasks(
    _flow_id,
   array_agg((_node, a)::flow.task_wrapper_t),
   _source := 'push steps')
  FROM unnest(_arguments) a;
END;    
$$ LANGUAGE PLPGSQL;


/* Return arguments from the flow.  If they are not in the local cache, go get
 * them from the orchestrator.
 */
CREATE OR REPLACE FUNCTION flow.args(
  _flow_id BIGINT,
  args OUT JSONB) RETURNS JSONB AS
$$
DECLARE
  q TEXT;
BEGIN
  SELECT INTO args arguments
  FROM flow.arguments
  WHERE flow_id = _flow_id;

  IF FOUND
  THEN
    RETURN;
  END IF;

  IF (SELECT client_only FROM async.client_control)
  THEN
    SELECT INTO args * FROM dblink(
      async.server(), 
      format(
        'SELECT arguments FROM flow.flow WHERE flow_id = %s',
        _flow_id)) AS R(j JSONB);
  ELSE
    SELECT INTO args arguments
    FROM flow.flow 
    WHERE flow_id = _flow_id;
  END IF;

  INSERT INTO flow.arguments
  SELECT _flow_id, args
  ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE PLPGSQL;

/* 
 * Will finish 'in-process' node. Useful when the node is set asynchronous, but
 * it is deterimined an asynchronous finish is not needed
 */
CREATE OR REPLACE FUNCTION flow.finish(
  _args flow.callback_arguments_t,
  _failed BOOL DEFAULT false,
  _error_message TEXT DEFAULT NULL) RETURNS VOID AS
$$
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    PERFORM * FROM dblink(
      async.server(), 
      format(
        'SELECT 0 FROM flow.finish(%s, %s, %s)',
        quote_literal($1),
        quote_literal($2),
        quote_nullable($3))) AS R(V int);

    RETURN;
  END IF; 

  PERFORM async.finish(
    array[task_id],
    CASE WHEN _failed THEN 'FAILED' ELSE 'FINISHED' END::async.finish_status_t,
      _error_message)
  FROM flow.v_flow_task
  WHERE 
    flow_id = _args.flow_id
    AND node = _args.node
    AND step_arguments = _args.step_arguments;
END;
$$ LANGUAGE PLPGSQL;



/* Marks a flow and all attached tasks as ineligible to run. Any tasks
 * running synchronously will be cancelled.
 */
CREATE OR REPLACE FUNCTION flow.cancel(
  _flow_id BIGINT) RETURNS VOID AS
$$
DECLARE
  _task_ids BIGINT[];
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    PERFORM * FROM dblink(
      async.server(), 
      format('SELECT 0 FROM flow.cancel(%s)', $1)) AS R(V INT);

    RETURN;
  END IF; 

  SELECT INTO _task_ids array_agg(task_id) 
  FROM flow.v_flow_task 
  WHERE 
    flow_id = _flow_id
    AND processed IS NULL;

  UPDATE flow.flow SET processed = clock_timestamp()
  WHERE flow_id = _flow_id;

  IF array_upper(_task_ids, 1) >= 1
  THEN
    /* If all tasks are complete, flow need to be marked cancelled only. 
     *
     * Having no tasks to cancel should be quite rare in regular practice as 
     * the cancel would have to have lost the race to the last task finishing. 
     * More likely, an open flow with no extant tasks would be due to bad state 
     * management or external manipulation of the task table.
     */
     PERFORM async.cancel(_task_ids, 'flow cancel');
  END IF;
END;
$$ LANGUAGE PLPGSQL;











