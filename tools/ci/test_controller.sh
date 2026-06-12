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

#
# This file contains generic control over the test execution.
#

HERE="`dirname \"$0\"`"             # relative
HERE="`( cd \"$HERE\" && pwd )`"    # absolutized and normalized
if [ -z "$HERE" ] ; then
	exit 1
fi

source "${HERE}/stage.sh"
source "${HERE}/maven-utils.sh"
source "${HERE}/controller_utils.sh"

STAGE=$1

# =============================================================================
# Step 0: Check & print environment information & configure env
# =============================================================================

# check preconditions
if [ -z "${DEBUG_FILES_OUTPUT_DIR:-}" ] ; then
	echo "ERROR: Environment variable 'DEBUG_FILES_OUTPUT_DIR' is not set but expected by test_controller.sh. Tests may use this location to store debugging files."
	exit 1
fi

if [ ! -d "$DEBUG_FILES_OUTPUT_DIR" ] ; then
	echo "ERROR: Environment variable DEBUG_FILES_OUTPUT_DIR=$DEBUG_FILES_OUTPUT_DIR points to a directory that does not exist"
	exit 1
fi

if [ -z "${STAGE:-}" ] ; then
	echo "ERROR: Environment variable 'STAGE' is not set but expected by test_controller.sh. THe variable refers to the stage being executed."
	exit 1
fi

echo "Printing environment information"

echo "PATH=$PATH"
run_mvn -version
echo "Commit: $(git rev-parse HEAD)"
print_system_info

# enable coredumps for this process
ulimit -c unlimited

# configure JVMs to produce heap dumps
export JAVA_TOOL_OPTIONS="-XX:+HeapDumpOnOutOfMemoryError"

# some tests provide additional logs if they find this variable
export IS_CI=true

export WATCHDOG_ADDITIONAL_MONITORING_FILES="$DEBUG_FILES_OUTPUT_DIR/mvn-*.log"

source "${HERE}/watchdog.sh"

# =============================================================================
# Step 1: Rebuild jars and install Flink to local maven repository
# =============================================================================

export LOG4J_PROPERTIES=${HERE}/log4j.properties
MVN_LOGGING_OPTIONS="-Dlog.dir=${DEBUG_FILES_OUTPUT_DIR} -Dlog4j.configurationFile=file://$LOG4J_PROPERTIES"

MVN_COMMON_OPTIONS="-Dfast -Pskip-webui-build $MVN_LOGGING_OPTIONS"
MVN_COMPILE_OPTIONS="-DskipTests"
MVN_COMPILE_MODULES=$(get_compile_modules_for_stage ${STAGE})

CALLBACK_ON_TIMEOUT="print_stacktraces | tee ${DEBUG_FILES_OUTPUT_DIR}/jps-traces.out"

# In GHA the compile job ships pre-built jars and pre-installs Flink's Maven artifacts, so
# test jobs can skip this rebuild/install by setting SKIP_REBUILD=true. Azure never sets
# SKIP_REBUILD and always rebuilds.
#
# The *scoped* stages (core/table/connect/tests, which compile a -pl subset) skip it with just the
# pre-installed Maven artifacts. python and misc additionally need the assembled distribution on
# disk -- python's lint-python.sh resolves FLINK_HOME from build-target, and misc's yarn/dist tests
# consume the distribution -- because flink-dist assembles from sibling modules' target/ jars by
# path, which are stripped from the shipped artifact. When the compile job's distribution has been
# restored (DIST_PRECOMPILED=true; build-target in place) they skip the rebuild too; otherwise they
# fall back to a full build that reassembles the distribution itself.
if [ "${SKIP_REBUILD:-false}" != "true" ] \
	|| { { [ "$STAGE" == "$STAGE_PYTHON" ] || [ "$STAGE" == "$STAGE_MISC" ]; } && [ "${DIST_PRECOMPILED:-false}" != "true" ]; }; then
	run_with_watchdog "run_mvn $MVN_COMMON_OPTIONS $MVN_COMPILE_OPTIONS $PROFILE $MVN_COMPILE_MODULES install" $CALLBACK_ON_TIMEOUT
	EXIT_CODE=$?

	if [ $EXIT_CODE != 0 ]; then
		echo "=============================================================================="
		echo "Compilation failure detected, skipping test execution."
		echo "=============================================================================="
		exit $EXIT_CODE
	fi
else
	echo "SKIP_REBUILD=true: using pre-built jars and pre-installed Flink artifacts from the compile job; skipping Step 1 rebuild/install."
fi


# =============================================================================
# Step 2: Run tests
# =============================================================================

if [ $STAGE == $STAGE_PYTHON ]; then
	sed -i "s/\(^appender\.file\.fileName = \).*$/\1\$\{sys:log\.file\}/g" ${HERE}/log4j.properties
	run_with_watchdog "./flink-python/dev/lint-python.sh" $CALLBACK_ON_TIMEOUT
	EXIT_CODE=$?
else
	MVN_TEST_OPTIONS="-Dflink.tests.with-openssl -Dflink.tests.check-segment-multiple-free -Darchunit.freeze.store.default.allowStoreUpdate=false -Dpekko.rpc.force-invocation-serialization"
	MVN_TEST_MODULES=$(get_test_modules_for_stage ${STAGE})

	# When misc skips the rebuild and runs against the restored distribution, flink-dist must not
	# re-run its bin/opt/plugins assembly: those read sibling modules' target/ jars by path, which
	# are stripped from the shipped artifact. Skipping only the assembly still lets flink-dist's
	# shade build its jars (flink-dist*.jar, bash-java-utils.jar) from the pre-installed Maven
	# artifacts, so BashJavaUtilsITCase still runs; the assembled distribution that downstream misc
	# tests (e.g. yarn) consume was restored from the compile job. flink-dist is the only module in
	# misc's scope that binds the assembly plugin. DIST_PRECOMPILED=true is only set alongside
	# SKIP_REBUILD=true, so the full-build fallback assembles the distribution as before.
	if [ "$STAGE" == "$STAGE_MISC" ] && [ "${DIST_PRECOMPILED:-false}" == "true" ]; then
		MVN_TEST_OPTIONS="$MVN_TEST_OPTIONS -Dassembly.skipAssembly=true"
	fi

	run_with_watchdog "run_mvn $MVN_COMMON_OPTIONS $MVN_TEST_OPTIONS $PROFILE $MVN_TEST_MODULES verify" $CALLBACK_ON_TIMEOUT
	EXIT_CODE=$?
fi

# =============================================================================
# Step 3: Put extra logs into $DEBUG_FILES_OUTPUT_DIR
# =============================================================================

# only misc builds flink-yarn-tests
case $STAGE in
	(misc)
		put_yarn_logs_to_artifacts
	;;
esac

collect_coredumps $(pwd) $DEBUG_FILES_OUTPUT_DIR

# Exit code for CI build success/failure
exit $EXIT_CODE
