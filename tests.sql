/* create a flow and execute it */

CREATE OR REPLACE PROCEDURE random_wait(
  _args flow.callback_arguments_t) AS 
$$
BEGIN
  PERFORM pg_sleep(random() * 10);
END;
$$ LANGUAGE PLPGSQL;




CREATE OR REPLACE PROCEDURE async_runner() AS
$$
DECLARE 
  r RECORD;
  _found BOOL;
  _error_message TEXT DEFAULT 'Simulated remote failure';
BEGIN
  PERFORM 1 FROM pg_stat_activity 
  WHERE 
    pid != pg_backend_pid()
    AND query ~* 'async_runner'
    AND state = 'active';
  IF FOUND
  THEN 
    RETURN;
  END IF;

  DROP TABLE IF EXISTS async_simulated_task;
  CREATE TABLE async_simulated_task
  (
    task_id BIGINT PRIMARY KEY,
    flow_data flow.callback_arguments_t,
    when_finshed TIMESTAMPTZ,
    should_fail BOOL
  );

  CREATE INDEX ON async_simulated_task(when_finshed);  

  COMMIT;

  LOOP
    _found := false;

    FOR r IN SELECT * FROM async_simulated_task
      WHERE when_finshed < now()  
      ORDER BY when_finshed
    LOOP
      _found := true;

      DELETE FROM async_simulated_task WHERE task_id = r.task_id;

      CALL flow.finish(
        r.flow_data,
        r.should_fail,
        _error_message);
    END LOOP;

    COMMIT;

    IF NOT _found 
    THEN
      PERFORM pg_sleep(.01);
    END IF;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE PROCEDURE random_wait_and_finish(
  _args flow.callback_arguments_t) AS 
$$
DECLARE
  _fail_pct FLOAT8 DEFAULT 
    COALESCE(
      (_args.flow_arguments->>'fail_pct')::FLOAT8,
      0.01);
  _scale INT DEFAULT 
    COALESCE(
      (_args.flow_arguments->>'scale')::INT,
      2);
  _steps JSONB[];

  _remote_fail BOOL;
BEGIN
  IF random() < _fail_pct
  THEN
    /* if even fail now, if odd, fail in remote call */
    IF right(random()::TEXT, 1)::INT % 2 = 0
    THEN
      RAISE EXCEPTION 'Simulated local failure';
    ELSE
      _remote_fail := true;
    END IF;
  END IF;

  IF _args.step_arguments = '{}'
  THEN
    SELECT INTO _steps array_agg(to_jsonb(s))
    FROM generate_series(1, (10 ^ (_scale - 1))::INT) s;

    CALL flow.push_steps(
      _args.flow_id,  
      _args.node,
      _steps);
  ELSE
    INSERT INTO async_simulated_task VALUES(
      _args.task_id,
      _args,
      now() + (greatest(random(), 0.0001)::TEXT || ' seconds')::INTERVAL,
      random() < _fail_pct);
  END IF;
END;
$$ LANGUAGE PLPGSQL;


CREATE TABLE IF NOT EXISTS test_flow_insert
(
  test_flow_insert_id BIGSERIAL PRIMARY KEY,
  inserted TIMESTAMPTZ DEFAULT clock_timestamp()
);

CREATE OR REPLACE PROCEDURE flow_insert_test_setup(
  _args flow.callback_arguments_t) AS 
$$
BEGIN
  TRUNCATE test_flow_insert;
END;
$$ LANGUAGE PLPGSQL;




CREATE OR REPLACE PROCEDURE flow_defer_task(
  _args flow.callback_arguments_t) AS 
$$
BEGIN
  CALL flow.defer(_args, (_args.flow_arguments->>'defer_for')::INTERVAL);
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE PROCEDURE flow_insert_test_run(
  _args flow.callback_arguments_t) AS 
$$
DECLARE
  _step_argments JSONB[];
BEGIN
  _step_argments := array(
      SELECT jsonb_build_object('id', s)
      FROM generate_series(1, (_args.flow_arguments->>'rows')::INT) s
    );

  CALL flow.push_steps(
    _args.flow_id,
    _args.node,
    _step_argments);
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE PROCEDURE flow_insert_test_insert(
  _args flow.callback_arguments_t) AS 
$$
BEGIN
  INSERT INTO test_flow_insert (test_flow_insert_id) 
  VALUES ((_args.step_arguments->>'id')::INT);
END;
$$ LANGUAGE PLPGSQL;


/*
 *
  select flow.create_test_flows('host=localhost port=5400 user=merlin.moncure dbname=postgres');
 */

CREATE OR REPLACE FUNCTION flow.create_test_flows(
  _self_target TEXT DEFAULT 
    'host=localhost port=5432 user=postgres dbname=postgres',
  _seed INT DEFAULT 0.5,
  _depth INT DEFAULT 5,
  _breadth INT DEFAULT 5) RETURNS VOID AS
$$
DECLARE 
  j JSON;
BEGIN
  /* Initialize targets.  Bronze/silver separated to mainly to allow for 
   * adjustment of worker pools.
   */
  PERFORM async.configure(format($j$
  {
    "targets": [
        { 
        "target": "SELF", 
        "max_concurrency": 50, 
        "default_timeout": "2 hours",
        "connection_string": "%1$s"
      },
      { 
        "target": "SELF_ASYNC", 
        "max_concurrency": 50, 
        "default_timeout": "2 hours",
        "connection_string": "%1$s",
        "asynchronous_finish": true,
        "concurrency_track_yielded": false
      }
    ],
    "control": {
      "self_connection_string": "%1$s",
      "default_timeout": "6 hours",
      "workers": 100
    }
  }$j$, 
    _self_target)::JSONB);

  PERFORM setseed(_seed);

  /* initialize flows */
  CREATE TEMP TABLE tmp_n AS
  WITH RECURSIVE nodes AS
  ( 
    SELECT 
      1 AS depth, 
      b AS breadth,
      NULL::TEXT AS parent,
      format('d%s.b%s', 1, b) AS child
    FROM generate_series(1, (random() * _breadth)::INT + 1) b
    UNION ALL SELECT 
      n.depth + 1 AS depth, 
      q.child AS breadth,
      n.child AS parent,
      format('%sd%sb%s', n.child, n.depth + 1, q.child) AS child
    FROM nodes n
    CROSS JOIN LATERAL 
    (
      SELECT 
        n.breadth AS parent,
        generate_series(1, (random() * 5)::INT + 1) AS child
    ) q
    WHERE n.depth < _depth AND (n.depth < 2 OR random() > .7)
  )
  SELECT * FROM nodes;

  PERFORM 
    flow.configure_flow(
      'flow_basic_test',
      jsonb_build_object(
        'nodes', 
        array
        (
          SELECT 
            jsonb_build_object(
              'node', 'basic_' || child,
              'routine', 'random_wait',
              'target', 'SELF',
              CASE WHEN parent IS NOT NULL THEN 'dependencies' ELSE 'dummy' END,
              array[
                  jsonb_build_object(
                    'parent', 
                    'basic_' || parent
                  )
              ]
            )
          FROM tmp_n
      )
    )
  );

  PERFORM flow.configure_flow(
    'flow_async_test',
    jsonb_build_object(
      'nodes', 
      array
      (
        SELECT 
          jsonb_build_object(
            'node', 'async_' || child,
            'routine', 'random_wait_and_finish',
            'synchronous', true,
            'target', 'SELF_ASYNC',
            'all_steps_most_complete', false,
            CASE WHEN parent IS NOT NULL THEN 'dependencies' ELSE 'dummy' END,
            array[
                jsonb_build_object(
                  'parent', 
                  'async_' || parent
                )
            ]
          )
        FROM tmp_n
      )
    )
  );

  DROP TABLE tmp_n;

  PERFORM flow.configure_flow(
    'flow_insert_test',
    $j$
  {
    "nodes": 
    [
      {
        "node": "flow_insert_test_setup",  
        "target": "SELF"
      },
      {
        "node": "flow_insert_test_run",
        "target": "SELF",
        "step_routine": "flow_insert_test_insert",
        "dependencies": [ {"parent": "flow_insert_test_setup"} ]
      }
    ]
  }$j$);

  PERFORM flow.configure_flow(
    'flow_defer_test',
    $j$
  {
    "nodes": 
    [
      {
        "node": "flow_defer_task",
        "target": "SELF",
        "node_timeout": "30 seconds"
      }
    ]
  }$j$);  

END;
$$ LANGUAGE PLPGSQL;

/*
SELECT flow.create_test_flows();

SELECT flow.create_flow('flow_insert_test', '{"rows": 100000}');

SELECT flow.create_flow('flow_defer_test', '{"defer_for": "1 second"}');


CALL async_runner();
*/

