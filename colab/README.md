# Nessie Demos - Jupyter notebooks

Remember that some public Jupyter notebook runtime environments
(Google Colaboratory) cannot access other files next to the notebook, only the
notebook itself.

To run a demo locally, you need a fresh "venv":

```bash
virtualenv -p $(which python3) venv
. venv/bin/activate
pip install --upgrade pip
pip install -r setup/requirements.txt
pip install jupyterlab
```

## Developer notes

