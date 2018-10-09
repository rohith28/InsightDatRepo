# InsightDatRepo

**Install Apache Airflow**

* add the following lines to `~/.bashrc` and source it

```
export AIRFLOW_HOME=~/airflow
```

* use `pip` to install Airflow

```
pip install apache-airflow --user
```

* Check Installation

```
airflow version
```

**Run Airflow DAGs**
* move to python scripts to `$AIRFLOW_HOME/dags/`
* execute the python script
```
python test.py
```
* Initiating Airflow Database
```
airflow initdb
```
* Launch webserver
```
airflow webserver -p 8118
```

* open scheduler
```
airflow scheduler
```

* To access the WebUI,  open the `http://<EC2 Public DNS>:8118`


