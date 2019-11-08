#!/bin/bash

mkdir -p avro_tools

# Avro-1.4.1 could not handle `alias` properly, so we choose to use avro-tools-1.4.0 to generate specific record
# to ignore `alias` fields.
DEFAULT_AVRO_TOOLS_140_JAR="avro_tools/avro_tools_140/avro-tools-1.4.0.jar"
DEFAULT_AVRO_TOOLS_177_JAR="avro_tools/avro_tools_177/avro-tools-1.7.7.jar"
DEFAULT_AVRO_TOOLS_182_JAR="avro_tools/avro_tools_182/avro-tools-1.8.2.jar"

if [ ! -f $DEFAULT_AVRO_TOOLS_140_JAR ]; then
  mkdir -p avro_tools/avro_tools_140 && pushd . && cd avro_tools/avro_tools_140
  curl -s -L -O -C - "https://repo1.maven.org/maven2/org/apache/avro/avro/1.4.0/avro-tools-1.4.0.jar" -o avro-tools-1.4.0.jar
  popd
fi
if [ ! -f $DEFAULT_AVRO_TOOLS_177_JAR ]; then
  mkdir -p avro_tools/avro_tools_177 && pushd . && cd avro_tools/avro_tools_177
  curl -s -L -O -C - "https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.7.7/avro-tools-1.7.7.jar" -o avro-tools-1.7.7.jar
  popd
fi
if [ ! -f $DEFAULT_AVRO_TOOLS_182_JAR ]; then
  mkdir -p avro_tools/avro_tools_182 && pushd . && cd avro_tools/avro_tools_182
  curl -s -L -O -C - "https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.8.2/avro-tools-1.8.2.jar" -o avro-tools-1.8.2.jar
  popd
fi

# schema path to compile
AVRO_SCHEMAS_PATH=(
  "src/test/avro/fastserdetest.avsc"
  "src/test/avro/defaultsTest.avsc"
  "src/test/avro/stringableTest.avsc"
)
# path to store the generated
CODE_GEN_PATH="src/test/java"

# full path to store the compiled classes, and the dimension of this array
# should be same as ${#AVRO_SCHEMAS_PATH[@]}
FULL_CODE_GEN_PATH=(
  "${CODE_GEN_PATH}/com/linkedin/avro/fastserde/generated/avro/*.java"
  "${CODE_GEN_PATH}/com/linkedin/avro/fastserde/generated/avro/*.java"
  "${CODE_GEN_PATH}/com/linkedin/avro/fastserde/generated/avro/*.java"
)

if [[ $# < 1 ]]; then
  echo "Usage: $0 avro_tools_path"
  echo ""
  echo "    avro_tools_path: full path to the avro-tools-1.x.x.jar file (required). If you use 'default_avro_140', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_140_JAR"
  echo ""
  echo "if you use 'default_avro_177', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_177_JAR"
  echo ""
  echo "if you use 'default_avro_182', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_182_JAR"
  echo ""
  echo "The $0 script uses avro-tools to generate SpecificRecord classes for the Avro schemas stored in:"
  echo ""
  for path in ${AVRO_SCHEMAS_PATH[@]}; do
      echo "    $path"
  done
  echo ""
  echo "The auto-generated classes are purged before each run and then re-generated here:"
  echo ""
  for path in ${FULL_CODE_GEN_PATH[@]}; do
      echo "    $path"
  done
  echo ""
  exit 1
fi

AVRO_TOOLS_PATH_PARAM=$1
echo $AVRO_TOOLS_PATH_PARAM

if [ "$AVRO_TOOLS_PATH_PARAM" = 'default_avro_140' ]; then
  AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_140_JAR
elif [ "$AVRO_TOOLS_PATH_PARAM" = 'default_avro_177' ]; then
    AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_177_JAR
elif [ "$AVRO_TOOLS_PATH_PARAM" = 'default_avro_182' ]; then
    AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_182_JAR
else
    AVRO_TOOLS_JAR=$AVRO_TOOLS_PATH_PARAM
fi

echo "Using AVRO_TOOLS_JAR=$AVRO_TOOLS_JAR"

for path in ${FULL_CODE_GEN_PATH[@]}; do
  rm -f $path
done

echo "Finished deleting old files. About to generate new ones..."

for (( i=0; i<${#FULL_CODE_GEN_PATH[@]}; i++ )); do
  java -jar $AVRO_TOOLS_JAR compile schema ${AVRO_SCHEMAS_PATH[i]} $CODE_GEN_PATH
  echo "Finished generation for schema path:  ${AVRO_SCHEMAS_PATH[i]}"
done

echo "Done!"
