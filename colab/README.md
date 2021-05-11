# Nessie Demos - Jupyter notebooks

Remember that some public Jupyter notebook runtime environments
(Google Colaboratory) cannot access other files next to the notebook, only the
notebook itself.

## Running locally with a "standalone Jupyter"

To run a demo locally with a "standalone Jupyter", you need a fresh "venv":

```bash
virtualenv -p $(which python3) venv
. venv/bin/activate
pip install --upgrade pip
pip install -r pydemolib/requirements.txt
pip install jupyterlab
```

## Running in Pycharm

You can use the same venv as mentioned above, actually. Pycharm should "just work".
