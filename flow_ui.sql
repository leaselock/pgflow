
/* Views and functions to support flow administration from UI */


/* get flow data and status */

CREATE OR REPLACE VIEW flow.v_flow_node_status AS
  SELECT 
    f.flow_id,
    f.flow,
    n.node,
    n2.target,
    t.consumed AS started,
    t.processed AS finished,
    CASE 
      WHEN t.finish_status = 'FINISHED' THEN 'Finished'
      WHEN t.processed IS NOT NULL THEN 'Failed'
      WHEN f.processed IS NOT NULL AND t.Consumed IS NULL THEN 'Cancelled'
      WHEN t.task_id IS NULL THEN 'Pending'
      WHEN t.yielded IS NOT NULL 
        AND COUNT(*) FILTER (WHERE step_id IS NOT NULL) > 0 THEN 'Running Steps'
      WHEN t.yielded IS NOT NULL THEN 'Running Async'
      WHEN t.consumed IS NOT NULL THEN 'Running'
      ELSE 'Unknown'
    END AS status,
    COUNT(*) FILTER (WHERE s.step_id IS NOT NULL) AS steps,
    COUNT(*) FILTER (WHERE s.Status = 'Pending') AS steps_pending,
    COUNT(*) FILTER (WHERE s.Status IN ('Running', 'Running Async')) AS steps_running,
    COUNT(*) FILTER (WHERE s.Status = 'Finished') AS steps_finished,
    COUNT(*) FILTER (WHERE s.Status = 'Failed') AS steps_failed
  FROM flow.flow f
  JOIN flow.flow_node n USING(flow)
  JOIN flow.node n2 USING(node)
  LEFT JOIN flow.v_flow_task t ON 
    f.flow_id = t.flow_id
    AND n.node = t.node
    AND flow.is_node(t.step_arguments)
  LEFT JOIN 
  (
    SELECT 
      t.flow_id,
      t.task_id AS step_id,
      t.node AS node,
      CASE 
        WHEN t.consumed IS NULL THEN 'Pending'
        WHEN t.processed IS NULL AND t.asynchronous_finish THEN 'Running Async'
        WHEN t.processed IS NULL THEN 'Running'
        WHEN t.finish_status = 'FINISHED' THEN 'Finished'
        WHEN t.processed IS NOT NULL THEN 'Failed'
        ELSE 'Unknown'
      END AS status
    FROM flow.v_flow_task t 
    WHERE NOT flow.is_node(t.step_arguments)
  ) s ON 
    s.flow_id = f.flow_id   
    AND s.node = n.node
  GROUP BY 1,2,3, n2.target, t.finish_status, t.processed, t.yielded, t.consumed, t.task_id;


CREATE OR REPLACE VIEW flow.v_flow_task_status AS
  SELECT 
    f.flow_id,
    t.task_id,
    CASE WHEN flow.is_node(t.step_arguments) THEN 'Node' ELSE 'Step' END AS Type,
    f.flow,
    n.node,
    n2.target,
    to_char(t.consumed, 'YYYY-MM-DD HH:MI:SS') AS started,
    CASE WHEN t.Processed IS NULL THEN 'No' ELSE 'Yes' END AS complete,
    interval_pretty(COALESCE(t.processed, now()) - t.consumed) AS run_time,
    CASE 
      WHEN t.finish_status = 'FINISHED' THEN 'Finished'
      WHEN t.processed IS NOT NULL THEN 'Failed'
      WHEN f.processed IS NOT NULL AND t.Consumed IS NULL THEN 'Cancelled'
      WHEN t.task_id IS NULL THEN 'Pending'
      WHEN t.consumed IS NULL THEN 'Pending'
      WHEN t.yielded IS NOT NULL 
        AND is_node THEN 'Running Steps'
      WHEN t.yielded IS NOT NULL THEN 'Running Async'
      WHEN t.consumed IS NOT NULL THEN 'Running'
      ELSE 'Unknown'
    END AS status,
    step_arguments::TEXT AS step_arguments,
    t.processing_error,
    replace(t.query, '##flow.TASK_ID##', t.task_id::TEXT) AS query
  FROM flow.flow f
  JOIN flow.flow_node n USING(flow)
  JOIN flow.node n2 USING(node)
  LEFT JOIN flow.v_flow_task t ON 
    f.flow_id = t.flow_id
    AND n.node = t.node;


  

CREATE OR REPLACE VIEW flow.v_flow_status_internal AS
  SELECT
    f.flow_id,
    f.flow,
    CASE WHEN f.Processed IS NULL THEN 'No' ELSE 'Yes' END AS complete,
    interval_pretty(COALESCE(f.processed, now()) - f.created) AS run_time,
    COUNT(*) AS count_nodes,
    COUNT(*) FILTER (WHERE NOT t.Failed) AS count_finished_nodes,
    COUNT(*) FILTER (WHERE t.Failed 
      OR (t.task_id IS NULL AND f.Processed IS NOT NULL)) AS count_failed_nodes,
    f.arguments,
    any_value(processing_error ORDER BY t.Processed) 
      FILTER (
        WHERE 
          failed 
          AND processing_error NOT LIKE 'Failed due to%'
          AND length(processing_error) > 0
        )
      AS first_error
  FROM flow.flow f
  LEFT JOIN flow.flow_node n USING(flow)
  LEFT JOIN flow.v_flow_task t ON
    f.flow_id = t.flow_id
    AND n.node = t.node 
    AND flow.is_node(t.step_arguments)
  GROUP BY 1,2;

CREATE OR REPLACE VIEW flow.v_flow_status AS
  SELECT
    f.flow_id,
    f.flow,
    CASE WHEN f.Processed IS NULL THEN 'No' ELSE 'Yes' END AS complete,
    interval_pretty(COALESCE(f.processed, now()) - f.created) AS run_time,
    count_nodes::BIGINT,
    count_finished_nodes::BIGINT,
    count_failed_nodes::BIGINT,
    f.arguments,
    first_error
  FROM flow.flow f
  WHERE count_nodes IS NOT NULL
  UNION ALL SELECT
    flow_id,
    flow,
    CASE WHEN Processed IS NULL THEN 'No' ELSE 'Yes' END AS complete,
    interval_pretty(COALESCE(processed, now()) - created) AS run_time,
    (i->>'count_nodes')::BIGINT,
    (i->>'count_finished_nodes')::BIGINT,
    (i->>'count_failed_nodes')::BIGINT,
    arguments,
    i->>'first_error'
  FROM 
  (
    SELECT 
      f.*, 
      (
        SELECT to_json(i)
        FROM flow.v_flow_status_internal i
        WHERE flow_id = f.flow_id      
      ) i
    FROM flow.flow f
    WHERE count_nodes IS NULL
  ) q;



CREATE OR REPLACE FUNCTION flow.flows(
  _limit INT DEFAULT 1000) RETURNS JSONB AS
$$
  SELECT jsonb_agg(s)
  FROM 
  (
    SELECT * 
    FROM flow.v_flow_status 
    ORDER BY flow_id DESC
    LIMIT _limit
  ) s;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION flow.task_list(_flow_id BIGINT) RETURNS JSONB AS
$$
  SELECT jsonb_agg(t)
  FROM 
  (
    SELECT * FROM flow.v_flow_task_status 
    WHERE 
      flow_id = _flow_id
      ORDER BY task_id DESC
  ) t;
$$ LANGUAGE SQL;




CREATE OR REPLACE FUNCTION flow.graphviz_node(
  _node TEXT,
  _target TEXT,
  _steps_overview TEXT,
  _runtime TEXT,
  _color TEXT,
  _pending BOOL,
  cell OUT TEXT) RETURNS TEXT AS
$$
DECLARE
  _cols TEXT;
BEGIN
  SELECT INTO _cols
    format('<TR><TD BGCOLOR="%s">%s</TD></TR>', _color, _target);

  SELECT INTO cell format($s$
    "%s" [id=%s
  label=<
    <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
    <TR><TD COLSPAN="1">%s</TD></TR>
    %s
    %s %s
    </TABLE>> shape=none  ];
    $s$,
    replace(_node, '.', '_'),
    replace(_node, '.', '_'),
    _node,
    _cols ,
    CASE WHEN _steps_overview != ''
      THEN format('<TR><TD COLSPAN="1">steps: %s</TD></TR>', _steps_overview)
      WHEN _pending THEN '<TR><TD COLSPAN="1">steps pending</TD></TR>'
      ELSE '<TR><TD COLSPAN="1">no steps</TD></TR>'
    END,
    CASE WHEN _runtime != ''
      THEN format('<TR><TD COLSPAN="1">%s</TD></TR>', _runtime)
      ELSE ''
    END);
  END ;
$$ LANGUAGE PLPGSQL;





CREATE OR REPLACE FUNCTION flow.graphviz(
  _flow_id BIGINT, Data OUT TEXT) RETURNS TEXT AS
$$
DECLARE
  _Tree TEXT;
  _Format TEXT;
  f flow.flow;

BEGIN
  SELECT INTO f * FROM flow.flow WHERE flow_id = _flow_id;

  WITH d AS
  (
    SELECT DISTINCT
      n.node,
      coalesce(p.parent, 'start') AS parent,
      coalesce(c.child, 'end') AS child,
      target,
      color,
      p.continue_on_failure,
      steps_overview,
      runtime,
      status
    FROM
    (
      SELECT
        flow_id,
        node,
        target,
        CASE WHEN Started IS NOT NULL 
          THEN interval_pretty(coalesce(finished, now()) - started)
          ELSE '' 
        END AS runtime,
        CASE status
          WHEN 'Pending' THEN 'beige'
          WHEN 'Running Async' THEN 'orange'
          WHEN 'Running Steps' THEN 'orange'
          WHEN 'Running' THEN 'orange'
          WHEN 'Finished' THEN 'palegreen'
          WHEN 'Cancelled' THEN 'indianred'
          WHEN 'Failed' THEN 'indianred'
          ELSE 'beige'
        END AS color,
        CASE WHEN steps > 0
          THEN
            format(
              'pending %s running %s finished %s failed %s',
              steps_pending,
              steps_running,
              steps_finished,
              steps_failed)
          ELSE ''
        END AS steps_overview,
        status
      FROM flow.v_flow_node_status
    ) n
    LEFT JOIN
    (
      SELECT d.flow_id, parent, child, continue_on_failure
      FROM flow.dependency d
      LEFT JOIN flow.v_flow_task t ON
        d.flow_id = t.flow_id
        AND d.parent = t.node
        AND NOT flow.is_node(t.step_arguments)
    ) p ON n.node = p.child AND n.flow_id = p.flow_id
    LEFT JOIN
    (
      SELECT d.flow_id, parent, child, continue_on_failure
      FROM flow.dependency d
      LEFT JOIN flow.v_flow_task t ON
        d.flow_id = t.flow_id
        AND d.child = t.node      
        AND NOT flow.is_node(t.step_arguments)
    ) c ON n.node = c.parent AND n.flow_id = c.flow_id
    WHERE n.flow_id = _flow_id
  )
  SELECT INTO _Tree, _Format
    string_agg(
      format('%s -> %s%s',
        replace(parent, '.', '_'),
        replace(node, '.', '_'),
        CASE WHEN continue_on_failure THEN ' [arrowhead=empty]' END), E'\n'
        ORDER BY parent, node),
    string_agg(
      flow.graphviz_node(
        node,
        target,
        steps_overview,
        runtime ,
        color,
        status = 'Pending') , E'\n'
        ORDER BY parent, node)
        FILTER(WHERE node != 'end')
  FROM
  (
    SELECT DISTINCT 
      parent, node, target, color, continue_on_failure, steps_overview, runtime, status
    FROM d 
    UNION ALL SELECT DISTINCT 
      node, 'end', target, color, false, steps_overview, runtime, status
    FROM d
    WHERE Child = 'end'
  ) q;

  SELECT INTO Data format($q$
digraph "%s" {
  edge [arrowsize="1.5"]

  %s%s%s

  start [shape=invtriangle label="%s"];
  end [shape=triangle label = "END"];

  legend  [shape=record];
  flow_pending [label = "Pending" shape="rectangle" style="striped" fillcolor = "beige"];
  flow_running [label = "Running" shape="rectangle"  style="striped" fillcolor = "orange"];
  flow_finished [label = "Finished" shape="rectangle"  style="striped" fillcolor = "palegreen"];
  flow_failed [label = "Failed" shape="rectangle"  style="striped" fillcolor = "indianred"];

  legend->flow_pending [arrowhead=empty label="  run on fail"];
  flow_pending->flow_running [label="  fail on fail"];
  flow_running->flow_finished;
  flow_running->flow_failed;

}$q$,
  format('%s/%s', f.flow, f.flow_id),
  _Tree,
  E'\n',
  _Format,
  format(E'%s\nid: %s', f.flow, f.flow_id));
END;
$$ LANGUAGE PLPGSQL;









