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

# Download wrapper used by the install-*.sh scripts: curl -L -s -S with a
# connect timeout, retrying the whole download on any failure.
#
# curl's own --retry does not cover errors such as "(35) OpenSSL SSL_connect:
# Connection reset by peer", and --retry-all-errors needs curl >= 7.71 (Ubuntu
# 20.04 ships 7.68), so retry in the shell instead.
#
# Usage: curl-retry.sh <curl args...>   (e.g. URL -o FILE)

attempts=5
delay=10

for ((attempt = 1; attempt <= attempts; attempt++)); do
  status=0
  curl -L -s -S --connect-timeout 30 "$@" || status=$?
  if [ "$status" -eq 0 ]; then
    exit 0
  fi
  echo "WARN: download failed with curl exit code $status (attempt $attempt/$attempts)" >&2
  if [ "$attempt" -lt "$attempts" ]; then
    sleep "$delay"
  fi
done

echo "ERROR: download failed after $attempts attempts: $*" >&2
exit 1
