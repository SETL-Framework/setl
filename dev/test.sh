#!/bin/bash

set -eux

export AWS_ACCESS_KEY_ID="fakeAccess"
export AWS_SECRET_ACCESS_KEY="fakeSecret"
export AWS_REGION="eu-west-1"

mvn ${MVN_OPT}
