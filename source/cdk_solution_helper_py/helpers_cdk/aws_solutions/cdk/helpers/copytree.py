# #####################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                 #
#                                                                                                                     #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. You may obtain a copy of the License at                                                          #
#                                                                                                                     #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                        #
#                                                                                                                     #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed   #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for  #
#  the specific language governing permissions and limitations under the License.                                     #
# #####################################################################################################################

import os
import shutil
from pathlib import Path


def ignore_globs(*globs):
    """Function that can be used as copytree() ignore parameter.

    Patterns is a sequence of glob-style patterns
    that are used to exclude files"""

    def _ignore_globs(path, names):
        ignored_names = []
        paths = [Path(os.path.join(path, name)).resolve() for name in names]
        for pattern in globs:
            for i, p in enumerate(paths):
                if p.match(pattern):
                    ignored_names.append(names[i])
        return set(ignored_names)

    return _ignore_globs


def copytree(src, dst, symlinks=False, ignore=None):
    if ignore:
        ignore.extend([ignored[:-2] for ignored in ignore if ignored.endswith("/*")])
    else:
        ignore = []

    if not os.path.exists(dst):
        os.makedirs(dst)

    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)

        # ignore full directories upfront
        if any(Path(s).match(ignored) for ignored in ignore):
            continue

        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore=ignore_globs(*ignore))
        else:
            shutil.copy2(s, d)
