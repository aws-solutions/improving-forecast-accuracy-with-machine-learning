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
from typing import List, Dict
from dataclasses import dataclass, field
from collections import UserList

import jmespath

ABSENT = "Absent"
PRESENT = "Present"
VALID_STATES = {ABSENT, PRESENT}
VALID_KEYS = {"Key", "Value", "State"}
DISALLOW_TAGS = {
    "SolutionId",
    "SolutionETag",
    "LatestDatasetUpdateETag",
    "LatestDatasetUpdateName",
}


def _get_present(tags: List[Dict]) -> List[Dict]:
    """
    Get a list of tags marked as present
    :param tags: The tags dictionary
    :return: the list of tags marked as present
    """
    return jmespath.search("[?State != `Absent`].{Key: Key, Value: Value || ``}", tags)


def _get_absent(tags: List[Dict]) -> List[Dict]:
    """
    Get a list of tags marked as absent
    :param tags: The tags dictionary
    :return: the list of tags marked as absent
    """
    return jmespath.search("[?State == `Absent`].Key", tags)


def _tags_as_dict(tags: List[Dict]) -> Dict[str, str]:
    """
    Convert a list of tags to a dictionary
    :param tags: the list of tags
    :return: the dictionary of tags
    """
    return {tag["Key"]: tag.get("Value", "") for tag in tags}


def _tags_as_list(tags: Dict[str, str]) -> List[Dict]:
    """
    Convert a dictionary of tags to a list of tags
    :param tags: the dictionary of tags
    :return: the list of tags
    """
    return [
        {
            "Key": key,
            "Value": value,
        }
        for key, value in tags.items()
    ]


def validate_tags(  # NOSONAR - allow higher complexity
    tags: List[Dict[str, str]]
) -> None:
    """
    Validate user-provided tags. Raises ValueError if there is an issue
    :param tags: the list of tags containing Key, Value, (optional) State
    :return: None
    """

    if not isinstance(tags, List):
        raise ValueError("tags must be a list")

    _keys = []
    for tag in tags:
        if not isinstance(tag, Dict):
            raise ValueError(
                "each tag must be a dictionary containing Key, Value, and optionally State"
            )

        tag_key = tag.get("Key", None)
        tag_value = tag.get("Value", "")
        tag_state = tag.get("State", PRESENT)

        if tag_key in _keys:
            raise ValueError(f"duplicate tag name {tag_key}")
        if tag_key in DISALLOW_TAGS:
            raise ValueError(
                f"you cannot set the following tags: {', '.join(DISALLOW_TAGS)}"
            )
        if not tag_key and tag_state == PRESENT:
            raise ValueError("each Present tag must have a Key")
        if not isinstance(tag_key, str):
            raise ValueError("each tag Key must be a string")
        if tag_state == PRESENT and not isinstance(tag_value, str):
            raise ValueError("all tag values must be a string")
        if not all(isinstance(v, str) for v in tag.values()):
            raise ValueError("Key, Value, and State must all be strings")
        if tag_state not in VALID_STATES:
            raise ValueError(f"tag State must be one of {','.join(VALID_STATES)}")
        if not all(k in VALID_KEYS for k in tag.keys()):
            raise ValueError(f"each tag must be one of {','.join(VALID_KEYS)}")
        _keys.append(tag_key)


def get_untags(
    resource_tags: List[Dict], global_tags: List[Dict], active_tags: List[Dict]
) -> List[str]:
    """
    Get the list of tag keys that should be removed
    :param resource_tags: the tags that should be associated with the resource
    :param global_tags: global tags to apply
    :param active_tags: the currently active tags
    :return: the list of tags to remove
    """
    validate_tags(resource_tags)
    validate_tags(global_tags)

    rp = set([tag["Key"] for tag in _get_present(resource_tags)])
    ra = set(_get_absent(resource_tags))
    ga = set(_get_absent(global_tags))
    at = set([tag["Key"] for tag in active_tags])
    ga = ga - rp.intersection(ga)
    return list(at.intersection(ga.union(ra)))


def get_tags(
    resource_tags: List[Dict], global_tags: List[Dict], active_tags: List[Dict]
) -> List[Dict]:
    """
    Get the list of tags that should be added
    :param resource_tags: the tags that should be associated with the resource
    :param global_tags: global tags to apply
    :param active_tags: the currently active tags
    :return: the list of tags to add
    """
    validate_tags(resource_tags)
    validate_tags(global_tags)

    rp: Dict = _tags_as_dict(_get_present(resource_tags))
    gp: Dict = _tags_as_dict(_get_present(global_tags))
    active_tags: Dict = _tags_as_dict(active_tags)

    gp = {**gp, **rp}
    return _tags_as_list({k: v for k, v in gp.items() if active_tags.get(k, None) != v})
