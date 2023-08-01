import logging
import subprocess as sp
from pathlib import Path
from re import sub

from git import Repo
from packaging import version

LOGGER = logging.Logger(__file__)


def submodule_has_diff(sm):
    if sm.parent_commit.hexsha != sm.hexsha:
        return True
    else:
        return False


def get_repositories(imported_package):
    """Fetches a list of the repositories for the project (includes its
    git submodules).

    Note: For now resort to a hack to detect the location of the git repository.
    This will trigger an error if the package is not installed in development
    mode. At some point that may be an issue.
    """
    repo = Repo(
        str(Path(imported_package.__file__).resolve().parent.parent),
        search_parent_directories=True,
    )
    # For info on working with submodules see:
    # https://gitpython.readthedocs.io/en/stable/tutorial.html#submodule-handling
    submodules = repo.submodules
    if any(not x.exists() for x in submodules):
        LOGGER.warning(
            "Some submodules do not exists! Consider running the command:\n"
            "git submodule update --init --recursive"
        )

    repositories = [repo, *[Repo(r.abspath) for r in submodules]]
    return repositories


def get_repo_info(repository):
    sm_diffs = any(
        submodule_has_diff(sm) for sm in repository.submodules if sm.exists()
    )
    try:
        branch_name = repository.active_branch.name
    except TypeError:
        branch_name = "N/A"

    info = {
        "is_dirty": repository.is_dirty(),
        "HEAD": repository.rev_parse("HEAD"),
        "commit_id": repository.rev_parse("HEAD").hexsha,
        "has_uncommitted_submodule_changes": sm_diffs,
        "branch_name": branch_name,
    }
    return info


def unpack_repo_info(metadata_dict):
    """
    Transforms a nested dictionary to be usable as Azure blob metadata.
    To upload metadata directly to the blob in Azure,
    it has to be in the form of a single, non-nested dictionary.
    Keys cannot have any dashes in them, so dashes in repo names
    get replaced by underscores.
    All dictionary values must be strings, so any non-string
    datatype gets cast as a string.
    """
    condensed_dict = {}
    for repo_name, info in metadata_dict.items():
        repo_name = repo_name.replace("-", "_")
        for key, val in info.items():
            condensed_dict[repo_name + "_" + key] = str(val)
    return condensed_dict


def get_version_info(imported_package):
    try:
        repositories = get_repositories(imported_package)
        version_info = {
            Path(repo.working_dir).name: get_repo_info(repo) for repo in repositories
        }
        condensed_info = unpack_repo_info(version_info)
    except Exception as err:
        LOGGER.warning(f"{err}\n\n Could not extract version information.")
        condensed_info = {}

    return condensed_info
