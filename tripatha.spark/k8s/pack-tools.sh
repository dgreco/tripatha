#!/usr/bin/env bash
#
# Copyright 2026 David Greco
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Packs what the driver pod runs into one tarball, for Accumulo's shared volume:
#
#   tripatha.spark/k8s/pack-tools.sh [OUT.tar.gz]     (default tripatha.spark/target/tripatha-tools.tar.gz)
#
# tools/tripatha.spark-job.jar   the Spark build's job jar
# tools/bin/loader, bin/bench    the loader (prepare, check, drop, ...) and the join benchmark, on Java 25
# tools/lib, loader-classes, loader-test-classes, classpath.txt   their classes and libraries
#
# It builds the modules first (mvn install, tests skipped).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
OUT="${1:-$ROOT/tripatha.spark/target/tripatha-tools.tar.gz}"
case "$OUT" in /*) ;; *) OUT="$PWD/$OUT" ;; esac

(cd "$ROOT" && mvn -B -q install -DskipTests -DskipITs)
CP_FILE="$(mktemp)"
(cd "$ROOT" && mvn -B -q -pl tripatha.loader test-compile dependency:build-classpath -Dmdep.includeScope=test \
    -Dmdep.outputFile="$CP_FILE")

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK" "$CP_FILE"' EXIT
T="$WORK/tools"
mkdir -p "$T/lib" "$T/bin"
cp -R "$ROOT/tripatha.loader/target/classes" "$T/loader-classes"
cp -R "$ROOT/tripatha.loader/target/test-classes" "$T/loader-test-classes"
cp "$ROOT"/tripatha.spark/target/tripatha.spark-*-job.jar "$T/tripatha.spark-job.jar"
: > "$T/classpath.txt"
tr ':' '\n' < "$CP_FILE" | while read -r jar; do
  case "$jar" in */classes|*/test-classes|"") continue ;; esac
  name="$(basename "$jar")"
  if [ ! -e "$T/lib/$name" ]; then
    cp "$jar" "$T/lib/$name"
    echo "lib/$name" >> "$T/classpath.txt"
  fi
done

cat > "$T/bin/tripatha-java" <<'EOF'
#!/bin/bash
# Runs a class of the loader (main or benchmark) with its classpath: tripatha-java CLASS ARGS...
T="$(cd "$(dirname "$0")/.." && pwd)"
CP="$T/loader-test-classes:$T/loader-classes"
while read -r j; do CP="$CP:$T/$j"; done < "$T/classpath.txt"
exec java --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED -Xmx${TRIPATHA_HEAP:-4g} \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=warn ${TRIPATHA_JAVA_OPTS:-} -cp "$CP" "$@"
EOF
printf '#!/bin/bash\nexec "$(dirname "$0")/tripatha-java" org.tripatha.loader.LoaderMain "$@"\n' > "$T/bin/loader"
printf '#!/bin/bash\nexec "$(dirname "$0")/tripatha-java" org.tripatha.loader.bench.JoinBench "$@"\n' > "$T/bin/bench"
chmod 755 "$T"/bin/*
(cd "$ROOT" && git rev-parse HEAD) > "$T/COMMIT"

mkdir -p "$(dirname "$OUT")"
# COPYFILE_DISABLE: no macOS extended attributes in the archive (GNU tar warns about each).
(cd "$WORK" && COPYFILE_DISABLE=1 tar czf "$OUT" tools)
echo "$OUT ($(du -h "$OUT" | cut -f1), $(wc -l < "$T/classpath.txt" | tr -d ' ') jars, commit $(cut -c1-7 "$T/COMMIT"))"
