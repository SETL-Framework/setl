#!/bin/bash

set -e

echo ${MVN_SETTINGS} | base64 -d > ${HOME}/.m2/settings.xml
echo ${MVN_SECURITY} | base64 -d > ${HOME}/.m2/settings-security.xml

mvn clean deploy -ntp -B -DskipTests -Psnapshot -P${SCALA_PROFILE}
