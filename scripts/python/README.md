# hadoop-ops-util in python
- It is recommended to manage Python versions using a package manager.
    - Recommend using a combination of `pyenv` and `virtualenv`.
- It is recommended to use the latest stable Python version.

```bash
## To use libraries without conflicts in different Python execution environments, set up pyenv and virtualenv
# Install pyenv
brew install pyenv

# Install pyenv virtualenv
brew install pyenv-virtualenv

# Create hadoop-ops-util environment; set `hadoop-ops-util` to use Python 3.8.12
pyenv virtualenv 3.8.12 hadoop-ops-util-env-3.8.12
 
# install hadoop-ops-util python libraries
cd scripts/python
pip install -r requirements.txt
```

## python package management tool install
- use pipx instead of pip

```bash
brew install pipx
```

## python project management tool install
```bash
pipx install poetry
```
