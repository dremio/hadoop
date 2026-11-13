#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Download wrapper used by the install-*.sh scripts: curl -L -s -S -f with a
# connect timeout, retrying the whole download on any failure.
#
# curl's own --retry does not cover errors such as "(35) OpenSSL SSL_connect:
# Connection reset by peer", and --retry-all-errors needs curl >= 7.71 (Ubuntu
# 20.04 ships 7.68), so retry in the shell instead.
#
# If GITHUB_MIRROR is set (e.g. https://nexus.example/repository/github-proxy/),
# a URL starting with https://github.com/ is first fetched from the mirror and
# only fetched from github.com if the mirror fails.
#
# Usage: curl-retry.sh <curl args...>   (e.g. URL -o FILE)

# download <attempts> <delay> <curl args...>
download() {
  local attempts=$1 delay=$2 attempt status
  shift 2

  for ((attempt = 1; attempt <= attempts; attempt++)); do
    status=0
    curl -L -s -S -f --connect-timeout 30 "$@" || status=$?
    if [ "$status" -eq 0 ]; then
      return 0
    fi
    echo "WARN: download failed with curl exit code $status (attempt $attempt/$attempts)" >&2
    if [ "$attempt" -lt "$attempts" ]; then
      sleep "$delay"
    fi
  done
  return 1
}

if [ -n "${GITHUB_MIRROR:-}" ]; then
  mirror_args=()
  use_mirror=false
  for arg in "$@"; do
    if [[ "$arg" == https://github.com/* ]]; then
      mirror_args+=("${GITHUB_MIRROR%/}/${arg#https://github.com/}")
      use_mirror=true
    else
      mirror_args+=("$arg")
    fi
  done

  if [ "$use_mirror" = true ]; then
    if download 3 5 "${mirror_args[@]}"; then
      exit 0
    fi
    echo "WARN: download from ${GITHUB_MIRROR} failed, falling back to github.com" >&2
  fi
fi

if download 5 10 "$@"; then
  exit 0
fi

echo "ERROR: download failed after 5 attempts: $*" >&2
exit 1
