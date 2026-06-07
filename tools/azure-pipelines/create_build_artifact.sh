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

echo "Copying build artifacts to directory $FLINK_ARTIFACT_DIR"

cp -r . "$FLINK_ARTIFACT_DIR"

echo "Minimizing artifact files"

# reduces the size of the artifact directory to speed up
# the packing&upload / download&unpacking process
# by removing files not required for subsequent stages

# jars are re-built in subsequent stages, so no need to cache them (cannot be avoided)
find "$FLINK_ARTIFACT_DIR" -maxdepth 8 -type f -name '*.jar' -exec rm -rf {} \;

# GHA (SHIP_PRECOMPILED): the compile job runs `install`, so the shade plugin emits
# dependency-reduced-pom.xml files next to module poms. They are generated (no license header)
# and would fail the apache-rat license check in the packaging stage, which scans the unpacked
# artifact tree. Drop them from the artifact. (Azure's compile does not run install, so none exist.)
if [ "${SHIP_PRECOMPILED:-false}" == "true" ]; then
  find "$FLINK_ARTIFACT_DIR" -type f -name 'dependency-reduced-pom.xml' -exec rm -f {} \;
fi

# .git directory
# not deleting this can cause build stability issues
# merging the cached version sometimes fails
rm -rf "$FLINK_ARTIFACT_DIR/.git"

# AZ Pipelines has a problem with links.
rm -f "$FLINK_ARTIFACT_DIR/build-target"

# Remove javadocs because they are not used in later stages
rm -rf "$FLINK_ARTIFACT_DIR/target/site"

# Remove WebUI node directories; unnecessary because the UI is already fully built
rm -rf "$FLINK_ARTIFACT_DIR/flink-runtime-web/web-dashboard/node"
rm -rf "$FLINK_ARTIFACT_DIR/flink-runtime-web/web-dashboard/node_modules"

if [ -n "${FLINK_ARTIFACT_FILENAME}" ]; then
  # GitHub Actions doesn't create an archive automatically - packaging the files improves the performance of artifact uploads
  echo "Archives artifacts into ${FLINK_ARTIFACT_DIR}/${FLINK_ARTIFACT_FILENAME}"
  tar --create --gzip --exclude "${FLINK_ARTIFACT_DIR}/${FLINK_ARTIFACT_FILENAME}" --file "${FLINK_ARTIFACT_FILENAME}" -C "${FLINK_ARTIFACT_DIR}" .
  mv "${FLINK_ARTIFACT_FILENAME}" "${FLINK_ARTIFACT_DIR}"
fi

# GHA only (SHIP_PRECOMPILED=true): additionally pack Flink's own installed Maven artifacts
# so downstream GHA test jobs can resolve them from the local repository and skip rebuilding
# and re-installing Flink in every stage. This runs after the main archive, so flink_m2.tar.gz
# is a separate payload (not nested in ${FLINK_ARTIFACT_FILENAME}). Azure leaves SHIP_PRECOMPILED
# unset and keeps rebuilding per stage, so its artifact is unchanged.
if [ "${SHIP_PRECOMPILED:-false}" == "true" ]; then
  echo "Packing Flink Maven artifacts from ${MAVEN_REPO_FOLDER}/org/apache/flink into ${FLINK_ARTIFACT_DIR}/flink_m2.tar.gz"
  tar --create --gzip --file "${FLINK_ARTIFACT_DIR}/flink_m2.tar.gz" -C "${MAVEN_REPO_FOLDER}" org/apache/flink
  # Also pack the assembled distribution (jars intact) for the e2e job's build-target. Taken from
  # the live working dir, since the copy in ${FLINK_ARTIFACT_DIR} had its jars stripped above.
  echo "Packing Flink distribution flink-dist/target into ${FLINK_ARTIFACT_DIR}/flink_dist.tar.gz"
  tar --create --gzip --file "${FLINK_ARTIFACT_DIR}/flink_dist.tar.gz" flink-dist/target
fi
