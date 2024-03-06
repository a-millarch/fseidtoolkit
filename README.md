# FSEID toolkit

Toolkit for FSEID-00006734 dump (early 2024)
- Create patient mapping from raw files
<br>> make mapping
- Change CPR to IHDAC PID for raw files using the mapping 
<br>> make pid

Make sure to update config file before running.

Docker: Run in interactive mode.

## Project structure

The directory structure of the project looks like this:

```txt

├── Makefile             <- Makefile with convenience commands like `make data` or `make train`
├── README.md            <- The top-level README for developers using this project.
├── data
│   ├── processed        <- The final, canonical data sets for modeling.
│   └── raw              <- The original, immutable data dump.
│
├── models               <- Trained and serialized models, model predictions, or model summaries
│
├── notebooks            <- Jupyter notebooks.
│
├── pyproject.toml       <- Project configuration file
│
├── requirements.txt     <- The requirements file for reproducing the analysis environment
│
├── src  <- Source code for use in this project.
│   │
│   ├── __init__.py      <- Makes folder a Python module
│   │
│   ├── data             <- Scripts to proces data
│   │   ├── __init__.py
│   │   ├── mapper.py
│   │   └── mapping_constructor.py
│   │
│   ├── utils            <- utilities
│   │   ├── __init__.py
│   │   ├── secret_pandas.py
│   │   └── utils.py
│   │
│   ├── common           <- Commonly used
│   │   ├── __init__.py
│   │   └── log_config.py
│
└── LICENSE              <- Open-source license if one is chosen
```