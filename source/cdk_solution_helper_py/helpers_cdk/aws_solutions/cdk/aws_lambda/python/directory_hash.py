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

import hashlib
import os
from pathlib import Path


class DirectoryHash:
    # fmt: off
    _hash = hashlib.sha1()  # NOSONAR - safe to hash; side-effect of collision is to create new bundle
    # fmt: on

    @classmethod
    def hash(cls, *directories: Path):
        DirectoryHash._hash = hashlib.sha1()  # NOSONAR - safe to hash; see above
        if isinstance(directories, Path):
            directories = [directories]
        for directory in sorted(directories):
            DirectoryHash._hash_dir(str(directory.absolute()))
        return DirectoryHash._hash.hexdigest()

    @classmethod
    def _hash_dir(cls, directory: Path):
        for path, dirs, files in os.walk(directory):
            for file in sorted(files):
                DirectoryHash._hash_file(Path(path) / file)
            for directory in sorted(dirs):
                DirectoryHash._hash_dir(str((Path(path) / directory).absolute()))
            break

    @classmethod
    def _hash_file(cls, file: Path):
        with file.open("rb") as f:
            while True:
                block = f.read(2 ** 10)
                if not block:
                    break
                DirectoryHash._hash.update(block)
