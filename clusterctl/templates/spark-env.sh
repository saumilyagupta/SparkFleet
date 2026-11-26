#!/usr/bin/env bash
# Managed by clusterctl. Manual edits are overwritten on the next provision.
# Host: {{host_name}} ({{host_ip}})

export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
export SPARK_HOME="{{spark_home}}"
export SPARK_MASTER_HOST="{{master_ip}}"
export SPARK_MASTER_PORT={{master_port}}
export SPARK_MASTER_WEBUI_PORT={{master_ui_port}}
export SPARK_WORKER_WEBUI_PORT={{worker_ui_port}}
# Bind to the LAN address, not 127.0.1.1 from Ubuntu's default /etc/hosts.
export SPARK_LOCAL_IP="{{host_ip}}"
export SPARK_LOCAL_HOSTNAME="{{host_ip}}"
export PYSPARK_PYTHON="{{python_bin}}"
export PYSPARK_DRIVER_PYTHON="{{python_bin}}"
export SPARK_WORKER_DIR="{{data_dir}}/work"
export SPARK_LOG_DIR="{{data_dir}}/logs"
{{worker_resources}}
