# AirflowExample
Small airflow example for BDMA Masters' BPM class at ULB.

# Installation 

Create a virtual environment for python using 
```sh
python -m venv venv
```

Activate it using 
```sh
source venv/bin/activate
```

Activate a couple of environment variables
```sh
export AIRFLOW_HOME=$(pwd)
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```
then install airflow with 
```sh
AIRFLOW_VERSION=2.7.3

PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```



Initialize airflow with
```sh
airflow standalone
```

You can check the current DAGs on `http://localhost:8080/`