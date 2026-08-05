#!/usr/bin/env bash

set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir="${ASTERRPC_BUILD_DIR:-${root_dir}/build-zk}"
compose_file="${root_dir}/deployments/zookeeper/compose.yaml"
temp_dir="$(mktemp -d)"
zookeeper_servers="${ASTERRPC_ZOOKEEPER_SERVERS:-127.0.0.1:2181}"
manage_zookeeper=true

if [[ -n "${ASTERRPC_ZOOKEEPER_SERVERS:-}" ]]; then
    manage_zookeeper=false
fi

server_9001_pid=""
server_9002_pid=""
server_9003_pid=""
client_pid=""

cleanup() {
    status=$?
    set +e

    for pid in \
        "${client_pid}" \
        "${server_9001_pid}" \
        "${server_9002_pid}" \
        "${server_9003_pid}"; do
        if [[ -n "${pid}" ]]; then
            kill "${pid}" 2>/dev/null
            wait "${pid}" 2>/dev/null
        fi
    done

    if [[ "${manage_zookeeper}" == "true" ]]; then
        docker compose -f "${compose_file}" down -v >/dev/null 2>&1
    fi
    if [[ "${status}" -eq 0 ]]; then
        find "${temp_dir}" -type f -delete
        rmdir "${temp_dir}" 2>/dev/null
    else
        echo "acceptance logs retained in ${temp_dir}" >&2
    fi
}
trap cleanup EXIT

if [[ "${manage_zookeeper}" == "true" ]]; then
    if ! docker info >/dev/null 2>&1; then
        echo "Docker daemon is unavailable for the current user" >&2
        echo "Set ASTERRPC_ZOOKEEPER_SERVERS to use an existing server" >&2
        exit 1
    fi
fi

if [[ "${ASTERRPC_SKIP_BUILD:-0}" != "1" ]]; then
    cmake \
        -S "${root_dir}" \
        -B "${build_dir}" \
        -DASTERRPC_WITH_ZOOKEEPER=ON
    cmake --build "${build_dir}" \
        --target registry_server registry_client \
        -j
fi

if [[ "${manage_zookeeper}" == "true" ]]; then
    docker compose -f "${compose_file}" up -d --wait
fi

"${build_dir}/registry_server" 9001 "${zookeeper_servers}" \
    >"${temp_dir}/server-9001.log" 2>&1 &
server_9001_pid=$!

"${build_dir}/registry_server" 9002 "${zookeeper_servers}" \
    >"${temp_dir}/server-9002.log" 2>&1 &
server_9002_pid=$!

sleep 2

"${build_dir}/registry_client" "${zookeeper_servers}" 80 250 \
    >"${temp_dir}/client.log" 2>&1 &
client_pid=$!

sleep 2
before_9003_line="$(wc -l <"${temp_dir}/client.log")"

"${build_dir}/registry_server" 9003 "${zookeeper_servers}" \
    >"${temp_dir}/server-9003.log" 2>&1 &
server_9003_pid=$!

sleep 3

kill -KILL "${server_9001_pid}"
wait "${server_9001_pid}" 2>/dev/null || true
server_9001_pid=""

# ZooKeeper需要经过Session超时才能删除被SIGKILL进程的临时节点。
sleep 8
converged_line="$(wc -l <"${temp_dir}/client.log")"

wait "${client_pid}"
client_pid=""

initial_output="$(
    sed -n "1,${before_9003_line}p" "${temp_dir}/client.log"
)"
if ! rg -q 'selected=127\.0\.0\.1:9001' <<<"${initial_output}"||
   ! rg -q 'selected=127\.0\.0\.1:9002' <<<"${initial_output}"; then
    echo "initial two-instance round robin failed" >&2
    sed -n '1,120p' "${temp_dir}/client.log" >&2
    exit 1
fi

if ! rg -q 'selected=127\.0\.0\.1:9003' \
    "${temp_dir}/client.log"; then
    echo "client did not discover the new 9003 provider" >&2
    exit 1
fi

final_output="$(
    tail -n "+$((converged_line+1))" "${temp_dir}/client.log"
)"
if rg -q 'selected=127\.0\.0\.1:9001' <<<"${final_output}"; then
    echo "9001 was still selected after convergence" >&2
    exit 1
fi
if ! rg -q 'providers=2' <<<"${final_output}"; then
    echo "provider snapshot did not converge to two instances" >&2
    exit 1
fi
if ! rg -q 'selected=127\.0\.0\.1:900[23]' <<<"${final_output}"; then
    echo "no remaining provider handled requests" >&2
    exit 1
fi

echo "ZooKeeper dynamic discovery acceptance passed"
echo
sed -n '1,160p' "${temp_dir}/client.log"
