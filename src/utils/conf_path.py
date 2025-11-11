#
# Copyright 2025 ABSA Group Limited
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

"""Module providing reusable configuration directory resolution.
Resolution order:
1. CONF_DIR env var if it exists and points to a directory
2. <project_root>/conf
3. <this_module_dir>/conf (flattened deployment)
4. Fallback to <project_root>/conf even if missing (subsequent file operations will raise)
"""

import os


def resolve_conf_dir(env_var: str = "CONF_DIR"):
    """Resolve the configuration directory path.

    Args:
        env_var: Name of the environment variable to check first (defaults to CONF_DIR).
    Returns:
        Tuple (conf_dir, invalid_env) where conf_dir is the chosen directory path and
        invalid_env is the rejected env var path if provided but invalid, else None.
    """
    # Simplified project root: two levels up from this file (../../)
    parent_utils_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    project_root = os.path.abspath(os.path.join(parent_utils_dir, ".."))
    current_dir = os.path.dirname(__file__)

    # Scenario 1: Environment variable if it exists and points to a directory
    env_conf = os.environ.get(env_var)
    invalid_env = None
    conf_dir = None

    if env_conf:
        candidate = os.path.abspath(env_conf)
        if os.path.isdir(candidate):
            conf_dir = candidate
        else:
            invalid_env = candidate

    # Scenario 2: Use <project_root>/conf if present and not already satisfied by env var
    if conf_dir is None:
        parent_conf = os.path.join(project_root, "conf")
        if os.path.isdir(parent_conf):
            conf_dir = parent_conf

    # Scenario 3: Use <this_module_dir>/conf for flattened deployments.
    if conf_dir is None:
        current_conf = os.path.join(current_dir, "conf")
        if os.path.isdir(current_conf):
            conf_dir = current_conf

    # Scenario 4: Final fallback to <project_root>/conf even if it does not exist.
    if conf_dir is None:
        conf_dir = os.path.join(project_root, "conf")

    return conf_dir, invalid_env


CONF_DIR, INVALID_CONF_ENV = resolve_conf_dir()

__all__ = ["resolve_conf_dir", "CONF_DIR", "INVALID_CONF_ENV"]
