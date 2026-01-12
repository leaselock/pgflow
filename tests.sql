/* create a flow and execute it */

CREATE OR REPLACE PROCEDURE random_wait(
  _args flow.callback_arguments_t) AS 
$$
BEGIN
  PERFORM pg_sleep(random() * 10);
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



CREATE OR REPLACE FUNCTION flow.create_test_flows(
  _self_target TEXT DEFAULT 
    'host=localhost port=5432 user=postgres dbname=postgres',
  _seed INT DEFAULT 0.5) RETURNS VOID AS
$$
DECLARE 
  j JSON;
  _depth INT DEFAULT 5;
  _breadth INT DEFAULT 5;
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
    FROM generate_series(1, (random() * 5)::INT + 1) b
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
    WHERE n.depth < 5 AND (n.depth < 2 OR random() > .7)
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
              'node', child,
              'routine', 'random_wait',
              'target', 'SELF',
              CASE WHEN parent IS NOT NULL THEN 'dependencies' ELSE 'dummy' END,
              array[
                  jsonb_build_object(
                    'parent', 
                    parent
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


SELECT flow.create_test_flows();

SELECT flow.create_flow('flow_insert_test', '{"rows": 100000}');

SELECT flow.create_flow('flow_defer_test', '{"defer_for": "1 second"}');
