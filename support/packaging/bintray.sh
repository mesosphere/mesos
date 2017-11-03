#!/bin/bash

#Sample Usage: pushToBintray.sh RPM

# Sample yum repo file:
# #bintraybintray-karya-mesos-centos-test - packages by karya from Bintray
# [bintraybintray-karya-mesos-centos-test]
# name=bintray-karya-mesos-centos-test
# baseurl=https://dl.bintray.com/karya/mesos-centos-test/el7/x86_64
# gpgcheck=0
# repo_gpgcheck=0
# enabled=1

set -o errexit -o nounset -o pipefail
set -x

#BINTRAY_USER
#BINTRAY_API_KEY

API=https://api.bintray.com
ORG=${BINTRAY_ORG:-mesos}
PKG=${BINTRAY_PKG:-mesos}

PKG_PATH=$1
PKG_FILENAME=$(basename $PKG_PATH)

REPO_SUFFIX=""
case "$PKG_FILENAME" in
  *.pre.*git*) REPO_SUFFIX="-unstable" ;;
  *-rc*)       REPO_SUFFIX="-testing" ;;
esac

REPO_BASE=""
VERSION=""
if [[ "$PKG_PATH" == *".rpm" ]]; then
  REPO="el${REPO_SUFFIX}"
  VERSION=$(rpm -qp ${PKG_PATH} --qf "%{VERSION}")
fi

REPO_PATH=""
case "$PKG_PATH" in
  *.el7.src.rpm)    REPO_PATH=7/SRPMS ;;
  *.el6.src.rpm)    REPO_PATH=6/SRPMS ;;

  *.el7.x86_64.rpm) REPO_PATH=7/x86_64 ;;
  *.el6.x86_64.rpm) REPO_PATH=6/x86_64 ;;
esac


CURL="curl \
  -u${BINTRAY_USER}:${BINTRAY_API_KEY} \
  -H Content-Type:application/json \
  -H Accept:application/json"

echo "Uploading ${PKG_FILENAME}..."

result=$(${CURL} \
  --write-out %{http_code} \
  --silent --output /dev/null \
  -T ${PKG_PATH} \
  -H X-Bintray-Package:${PKG} \
  -H X-Bintray-Version:${VERSION} \
  ${API}/content/${ORG}/${REPO}/${REPO_PATH}/${PKG_FILENAME})

if [ $result -ne 201 ]; then
  echo "Package ${PKG_FILENAME} upload failed!"
  exit 1
fi

echo "Publishing ${PKG_FILENAME}..."

result=""
if [[ "$PKG_PATH" == *".rpm" ]]; then
  result=$(${CURL} \
    -X POST ${API}/content/${ORG}/${REPO}/${PKG}/${VERSION}/publish \
    -d "{ \"discard\": \"false\" }")
  echo $result
fi
