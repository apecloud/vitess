#!/bin/bash
set -ex

vt_base_version='latest'
debian_versions='bullseye'
default_debian_version='bullseye'

if [[ "$(docker images -q apecloud/vitessbase:$vt_base_version 2> /dev/null)" == "" ]]; then
  docker pull --platform linux/amd64 apecloud/vitessbase:$vt_base_version
fi

for debian_version in $debian_versions
do
  echo "####### Building vitess/vt:$debian_version"

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t apecloud/vitesskubeblocks:$vt_base_version-$debian_version kubeblocks
  docker tag apecloud/vitesskubeblocks:$vt_base_version-$debian_version apecloud/vitesskubeblocks:$vt_base_version
#  docker push apecloud/vitesskubeblocks:$vt_base_version-$debian_version
#  if [[ $debian_version == $default_debian_version ]]; then docker push apecloud/vitesskubeblocks:$vt_base_version; fi

## TODO
#  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t apecloud/vtadmin:$vt_base_version-$debian_version kubeblocks/vtadmin
#  docker tag apecloud/vtadmin:$vt_base_version-$debian_version apecloud/vtadmin:$vt_base_version
#  docker push apecloud/vtadmin:$vt_base_version-$debian_version
#  if [[ $debian_version == $default_debian_version ]]; then docker push apecloud/vtadmin:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t apecloud/vtgate:$vt_base_version-$debian_version kubeblocks/vtgate
  docker tag apecloud/vtgate:$vt_base_version-$debian_version apecloud/vtgate:$vt_base_version
#  docker push apecloud/vtgate:$vt_base_version-$debian_version
#  if [[ $debian_version == $default_debian_version ]]; then docker push apecloud/vtgate:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t apecloud/vttablet:$vt_base_version-$debian_version kubeblocks/vttablet
  docker tag apecloud/vttablet:$vt_base_version-$debian_version apecloud/vttablet:$vt_base_version
#  docker push apecloud/vttablet:$vt_base_version-$debian_version
#  if [[ $debian_version == $default_debian_version ]]; then docker push apecloud/vttablet:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t apecloud/vtctl:$vt_base_version-$debian_version kubeblocks/vtctl
  docker tag apecloud/vtctl:$vt_base_version-$debian_version apecloud/vtctl:$vt_base_version
#  docker push apecloud/vtctl:$vt_base_version-$debian_version
#  if [[ $debian_version == $default_debian_version ]]; then docker push apecloud/vtctl:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t apecloud/vtctlclient:$vt_base_version-$debian_version kubeblocks/vtctlclient
  docker tag apecloud/vtctlclient:$vt_base_version-$debian_version apecloud/vtctlclient:$vt_base_version
#  docker push apecloud/vtctlclient:$vt_base_version-$debian_version
#  if [[ $debian_version == $default_debian_version ]]; then docker push apecloud/vtctlclient:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t apecloud/vtctld:$vt_base_version-$debian_version kubeblocks/vtctld
  docker tag apecloud/vtctld:$vt_base_version-$debian_version apecloud/vtctld:$vt_base_version
#  docker push apecloud/vtctld:$vt_base_version-$debian_version
#  if [[ $debian_version == $default_debian_version ]]; then docker push apecloud/vtctld:$vt_base_version; fi

  docker build --platform linux/amd64 --build-arg VT_BASE_VER=$vt_base_version --build-arg DEBIAN_VER=$debian_version-slim -t apecloud/vtconsensus:$vt_base_version-$debian_version kubeblocks/vtconsensus
  docker tag apecloud/vtconsensus:$vt_base_version-$debian_version apecloud/vtconsensus:$vt_base_version
#  docker push apecloud/vtconsensus:$vt_base_version-$debian_version
#  if [[ $debian_version == $default_debian_version ]]; then docker push apecloud/vtconsensus:$vt_base_version; fi
done
