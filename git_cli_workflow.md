# Git prerequisites lab — CLI workflow

This document reproduces the Git operations from the GitHub Desktop exercise with the command-line interface.

## Clone the repository

```bash
git clone https://github.com/Alfred0404/Labs.git
cd Labs
```

## Create and publish the integration branch

```bash
git switch -c develop
git add README.md .gitignore git_cli_workflow.md
git commit -m "docs: prepare repository workflow"
git push -u origin develop
```

Another group member can retrieve it with:

```bash
git fetch origin
git switch --track origin/develop
```

## Work on a feature branch

Each member creates a branch from the latest `develop` branch:

```bash
git switch develop
git pull --ff-only
git switch -c dev-firstname
# Edit a file, then stage and commit the change.
git add README.md
git commit -m "docs: add firstname contribution"
git push -u origin dev-firstname
```

## Merge a feature branch

```bash
git switch develop
git merge --no-ff dev-firstname
git push origin develop
```

## Resolve a merge conflict

When two branches change the same lines, Git marks the conflicting section with `<<<<<<<`, `=======` and `>>>>>>>`.
Edit the file to keep the intended final content, then finish the merge:

```bash
git status
git add README.md
git commit
git push origin develop
```

The conflict is resolved only when `git status` no longer reports unmerged paths and no conflict marker remains in the files.

## Code review

After pushing the feature branch, open a pull request on GitHub with `develop` as the target branch. Request the other group member as reviewer, apply any requested changes, and merge only after approval.

Useful checks before opening the pull request:

```bash
git status
git diff develop...HEAD
git log --oneline --graph --decorate --all
```

The review and approval must be performed by the other group member on GitHub; they cannot be reproduced truthfully by a local command.
