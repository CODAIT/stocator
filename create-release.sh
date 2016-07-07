#!/usr/bin/env bash

#
# (C) Copyright IBM Corp. 2015, 2016
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
#

function exit_with_usage {
  cat << EOF

create-release - Creates a release and publish maven artifacts from HEAD.

SYNOPSIS

usage: create-release.sh [--releaseVersion] [--developmentVersion] [--dryRun]

DESCRIPTION

Use maven infrastructure to create a project release and publish maven artifacts.

OPTIONS

--releaseVersion     - Release identifier used when publishing
--developmentVersion - Release identifier used for next development cyce
--dryRun             - Dry run only, mostly used for testing.

A GPG passphrase is expected as an environment variable

GPG_PASSPHRASE - Passphrase for GPG key used to sign release

EXAMPLES

create-release.sh --releaseVersion="1.0.4" --developmentVersion="1.0.5-SNAPSHOT" [--dryRun]

EOF
  exit 1
}

set -e

if [ $# -eq 0 ]; then
  exit_with_usage
fi


# Process each provided argument configuration
while [ "${1+defined}" ]; do
  IFS="=" read -ra PARTS <<< "$1"
  case "${PARTS[0]}" in
    --gitCommitHash)
      GIT_REF="${PARTS[1]}"
      shift
      ;;
    --releaseVersion)
      RELEASE_VERSION="${PARTS[1]}"
      shift
      ;;
    --developmentVersion)
      DEVELOPMENT_VERSION="${PARTS[1]}"
      shift
      ;;
    --dryRun)
      DRY_RUN="-DdryRun=true"
      shift
      ;;

    *help* | -h)
      exit_with_usage
     exit 0
     ;;
    -*)
     echo "Error: Unknown option: $1" >&2
     exit 1
     ;;
    *)  # No more options
     break
     ;;
  esac
done


for env in GPG_PASSPHRASE; do
  if [ -z "${!env}" ]; then
    echo "ERROR: $env must be set to run this script"
    exit_with_usage
  fi
done

if [[ -z "$RELEASE_VERSION" ]]; then
    echo "ERROR: --releaseVersion must be passed as an argument to run this script"
    exit_with_usage
fi

if [[ -z "$DEVELOPMENT_VERSION" ]]; then
    echo "ERROR: --developmentVersion must be passed as an argument to run this script"
    exit_with_usage
fi

# Explicitly set locale in order to make `sort` output consistent across machines.
# See https://stackoverflow.com/questions/28881 for more details.
export LC_ALL=C

if [ -z "$RELEASE_TAG" ]; then
  RELEASE_TAG="v$RELEASE_VERSION"
fi


echo "  "
echo "-------------------------------------------------------------"
echo "------- Release preparation with the following parameters ---"
echo "-------------------------------------------------------------"
echo "release version     ==> $RELEASE_VERSION"
echo "development version ==> $DEVELOPMENT_VERSION"
echo "tag                 ==> $RELEASE_TAG"
if [ "$DRY_RUN" ]; then
   echo "dry run ?           ==> true"
fi
echo "  "


echo "Preparing release $RELEASE_VERSION"

# Build and prepare the release
mvn -Pdistribution clean install
mvn -Pdistribution -DaltDeploymentRepository=sonatype-nexus-staging::default::https://oss.sonatype.org/service/local/staging/deploy/maven2 release:prepare release:perform gpg:sign $DRY_RUN -Dgpg.passphrase="$GPG_PASSPHRASE" -DskipTests -DreleaseVersion="$RELEASE_VERSION" -DdevelopmentVersion="$DEVELOPMENT_VERSION" -Dtag="$RELEASE_TAG"

exit 0
