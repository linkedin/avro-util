#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

PROJECT_VERSION=$( $SCRIPT_DIR/../gradlew :printVersion | grep -v \> | head -2 )
# remove leading whitespace characters
PROJECT_VERSION="${PROJECT_VERSION#"${PROJECT_VERSION%%[![:space:]]*}"}"
# remove trailing whitespace characters
PROJECT_VERSION="${PROJECT_VERSION%"${PROJECT_VERSION##*[![:space:]]}"}"

JAR_NAME="avro-builder-${PROJECT_VERSION}-all.jar"
java -jar $SCRIPT_DIR/build/libs/$JAR_NAME