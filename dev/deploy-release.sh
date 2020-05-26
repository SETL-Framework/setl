#!/usr/bin/env bash

set -e

echo ${MVN_SETTINGS} | base64 -d > ${HOME}/.m2/settings.xml
echo ${MVN_SECURITY} | base64 -d > ${HOME}/.m2/settings-security.xml
echo ${GPG_KEY} | base64 -d | gpg --import --batch > /dev/null 2>&1

mvn clean deploy scala:doc -ntp -B -DskipTests -P release,spark_${SPARK_VER}
