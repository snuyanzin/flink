#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# Prunes the local Maven repository before the cache task's post-job save step,
# so the persisted cache stays small. Designed to be safe to run multiple times
# and to no-op cleanly if the repo is missing.
#
# Usage:
#   ./prune_m2_cache.sh <maven-repo-path>
# Example:
#   ./prune_m2_cache.sh "$(MAVEN_CACHE_FOLDER)"

set -uo pipefail
REPO="${1:?usage: prune_m2_cache.sh <maven-repo-path>}"

if [ ! -d "$REPO" ]; then
  echo "[prune_m2_cache] No repo at $REPO, nothing to prune."
  exit 0
fi

echo "[prune_m2_cache] Maven repo size BEFORE prune:"
du -sh "$REPO" 2>/dev/null | head -1 || true

# 1. Drop artifact variants nobody resolves at runtime.
find "$REPO" -type f \( \
    -name '*-sources.jar' \
    -o -name '*-javadoc.jar' \
    -o -name '*.lastUpdated' \
    -o -name '_remote.repositories' \
    -o -name 'resolver-status.properties' \
  \) -delete 2>/dev/null || true

# 2. Strip everything outside groupIds Flink consumes. Adjust the keep-list if a
#    build later starts failing because a needed group was pruned.
find "$REPO" -mindepth 2 -maxdepth 2 -type d 2>/dev/null \
  | grep -vE '/(org/apache|org/codehaus|org/scala-lang|com/fasterxml|com/google|io/netty|io/confluent|io/dropwizard|net/bytebuddy|net/sf|com/typesafe)$' \
  | xargs -r rm -rf 2>/dev/null || true

# 3. Drop SNAPSHOT directories not modified in 14 days. Caps long-tail growth on
#    long-lived branches where snapshot versions accumulate over time.
find "$REPO" -type d -name '*-SNAPSHOT' -mtime +14 -exec rm -rf {} + 2>/dev/null || true

# 4. Drop empty directories left behind by the steps above.
find "$REPO" -mindepth 1 -type d -empty -delete 2>/dev/null || true

echo "[prune_m2_cache] Maven repo size AFTER prune:"
du -sh "$REPO" 2>/dev/null | head -1 || true
