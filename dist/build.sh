#!/bin/bash

cat ../flow.sql > scripts/flow_client_full.sql
cat ../flow.sql ../flow_server.sql ../flow_ui.sql > scripts/flow_server_full.sql

cp scripts/flow_client_full.sql extension/client/flow_client--1.0.sql
cp scripts/flow_server_full.sql extension/server/flow--1.0.sql
