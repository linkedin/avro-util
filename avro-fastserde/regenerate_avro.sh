#!/bin/bash

mkdir -p avro_tools

# Avro-1.4.1 could not handle `alias` properly, so we choose to use avro-tools-1.4.0 to generate specific record
# to ignore `alias` fields.
DEFAULT_AVRO_TOOLS_140_JAR="avro_tools/avro_tools_140/avro-tools-1.4.0.jar"
DEFAULT_AVRO_TOOLS_154_JAR="avro_tools/avro_tools_154/avro-tools-1.5.4.jar"
DEFAULT_AVRO_TOOLS_163_JAR="avro_tools/avro_tools_163/avro-tools-1.6.3.jar"
DEFAULT_AVRO_TOOLS_177_JAR="avro_tools/avro_tools_177/avro-tools-1.7.7.jar"
DEFAULT_AVRO_TOOLS_182_JAR="avro_tools/avro_tools_182/avro-tools-1.8.2.jar"
DEFAULT_AVRO_TOOLS_192_JAR="avro_tools/avro_tools_192/avro-tools-1.9.2.jar"
DEFAULT_AVRO_TOOLS_1100_JAR="avro_tools/avro_tools_1100/avro-tools-1.10.2.jar"
DEFAULT_AVRO_TOOLS_1110_JAR="avro_tools/avro_tools_1110/avro-tools-1.11.0.jar"

if [ ! -f $DEFAULT_AVRO_TOOLS_140_JAR ]; then
  mkdir -p avro_tools/avro_tools_140 && pushd . && cd avro_tools/avro_tools_140
  curl -s -L -O -C - "https://repo1.maven.org/maven2/org/apache/avro/avro/1.4.0/avro-tools-1.4.0.jar" -o avro-tools-1.4.0.jar
  popd
fi
if [ ! -f $DEFAULT_AVRO_TOOLS_154_JAR ]; then
  mkdir -p avro_tools/avro_tools_154 && pushd . && cd avro_tools/avro_tools_154
  curl -s -L -O -C - "https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.5.4/avro-tools-1.5.4.jar" -o avro-tools-1.5.4.jar
  popd
fi
if [ ! -f $DEFAULT_AVRO_TOOLS_163_JAR ]; then
  mkdir -p avro_tools/avro_tools_163 && pushd . && cd avro_tools/avro_tools_163
  curl -s -L -O -C - "https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.6.3/avro-tools-1.6.3.jar" -o avro-tools-1.6.3.jar
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
if [ ! -f $DEFAULT_AVRO_TOOLS_192_JAR ]; then
  mkdir -p avro_tools/avro_tools_192 && pushd . && cd avro_tools/avro_tools_192
  curl -s -L -O -C - "https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.9.2/avro-tools-1.9.2.jar" -o avro-tools-1.9.2.jar
  popd
fi
if [ ! -f $DEFAULT_AVRO_TOOLS_1100_JAR ]; then
  mkdir -p avro_tools/avro_tools_1100 && pushd . && cd avro_tools/avro_tools_1100
  curl -s -L -O -C - "https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.10.2/avro-tools-1.10.2.jar" -o avro-tools-1.10.2.jar
  popd
fi
if [ ! -f $DEFAULT_AVRO_TOOLS_1110_JAR ]; then
  mkdir -p avro_tools/avro_tools_1110 && pushd . && cd avro_tools/avro_tools_1110
  curl -s -L -O -C - "https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.11.0/avro-tools-1.11.0.jar" -o avro-tools-1.11.0.jar
  popd
fi


# schema path to compile
AVRO_SCHEMAS_PATH=(
  "src/test/avro/splitRecordTest1.avsc"
  "src/test/avro/splitRecordTest2.avsc"
  "src/test/avro/fastserdetest.avsc"
  "src/test/avro/defaultsTest.avsc"
  "src/test/avro/stringableTest.avsc"
  "src/test/avro/benchmarkSchema.avsc"
)
AVRO_SCHEMAS_ROOT_PATH="src/test/avro"

# path to store the generated
CODE_GEN_PATH="src/test/java"

# full path to store the compiled classes, and the dimension of this array
# should be same as ${#AVRO_SCHEMAS_PATH[@]}
FULL_CODE_GEN_PATH=(
  "${CODE_GEN_PATH}/com/linkedin/avro/fastserde/generated/avro/*.java"
  "${CODE_GEN_PATH}/com/linkedin/avro/fastserde/generated/avro/*.java"
  "${CODE_GEN_PATH}/com/linkedin/avro/fastserde/generated/avro/*.java"
  "${CODE_GEN_PATH}/com/linkedin/avro/fastserde/generated/avro/*.java"
  "${CODE_GEN_PATH}/com/linkedin/avro/fastserde/generated/avro/*.java"
  "${CODE_GEN_PATH}/com/linkedin/avro/fastserde/generated/avro/*.java"
)
FULL_CODE_GEN_ROOT_PATH="${CODE_GEN_PATH}/com/linkedin/avro/fastserde/generated/avro"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 avro_tools_path"
  echo ""
  echo "    avro_tools_path: full path to the avro-tools-1.x.x.jar file (required). If you use 'default_avro_140', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_140_JAR"
  echo ""
  echo "if you use 'default_avro_154', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_154_JAR"
  echo ""
  echo "if you use 'default_avro_177', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_163_JAR"
  echo ""
  echo "if you use 'default_avro_163', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_177_JAR"
  echo ""
  echo "if you use 'default_avro_182', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_182_JAR"
  echo ""
  echo "if you use 'default_avro_192', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_192_JAR"
  echo ""
  echo "if you use 'default_avro_1100', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_1100_JAR"
  echo ""
  echo "if you use 'default_avro_1110', it will take:"
  echo ""
  echo "$DEFAULT_AVRO_TOOLS_1110_JAR"
  echo ""
  echo "The $0 script uses avro-tools to generate SpecificRecord classes for the Avro schemas stored in:"
  echo ""
  for path in `ls ${AVRO_SCHEMAS_ROOT_PATH}/*.avsc`; do
    echo "    $path"
  done
  echo ""
  echo "The auto-generated classes are purged before each run and then re-generated here:"
  echo ""
  for path in `ls ${FULL_CODE_GEN_ROOT_PATH}/*.java`; do
    echo "    $path"
  done
  echo ""
  exit 1
fi

AVRO_TOOLS_PATH_PARAM=$1
echo $AVRO_TOOLS_PATH_PARAM

if [ "$AVRO_TOOLS_PATH_PARAM" = 'default_avro_140' ]; then
  AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_140_JAR
elif [ "$AVRO_TOOLS_PATH_PARAM" = 'default_avro_154' ]; then
    AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_154_JAR
elif [ "$AVRO_TOOLS_PATH_PARAM" = 'default_avro_163' ]; then
    AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_163_JAR
elif [ "$AVRO_TOOLS_PATH_PARAM" = 'default_avro_177' ]; then
    AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_177_JAR
elif [ "$AVRO_TOOLS_PATH_PARAM" = 'default_avro_182' ]; then
    AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_182_JAR
elif [ "$AVRO_TOOLS_PATH_PARAM" = 'default_avro_192' ]; then
    AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_192_JAR
elif [ "$AVRO_TOOLS_PATH_PARAM" = 'default_avro_1100' ]; then
    AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_1100_JAR
elif [ "$AVRO_TOOLS_PATH_PARAM" = 'default_avro_1110' ]; then
    AVRO_TOOLS_JAR=$DEFAULT_AVRO_TOOLS_1110_JAR
else
    AVRO_TOOLS_JAR=$AVRO_TOOLS_PATH_PARAM
fi

for file in `ls ${FULL_CODE_GEN_ROOT_PATH}/*.java`; do
  rm -f ${file}
done

echo "Finished deleting old files. About to generate new ones using $AVRO_TOOLS_JAR..."

for file in `ls ${AVRO_SCHEMAS_ROOT_PATH}/*.avsc`; do
  echo ${file}
  java -jar $AVRO_TOOLS_JAR compile schema ${file} $CODE_GEN_PATH
  echo "Finished generation for schema path:  ${file}"
done

echo "Done!"
