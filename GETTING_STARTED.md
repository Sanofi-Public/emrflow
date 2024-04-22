# Getting Started with EMRFLOW in Your Projects

When you're ready to run and monitor a job in EMR, you may encounter various scenarios. Here's a guide to help you navigate through them:


## Scenario 1: Main Script Contains Pure PySpark Code and Is Standalone
> [!TIP]
> - There's no need to build the dependency package (library package or Python module package) using the `package-dependencies` command.
> - Simply submit the job using the `run` command.

Example `emr_job.py` script:
```python
# main.py
import sys
from random import random
from operator import add
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
    Usage: pi [partitions]
    """
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n_val = 100000 * partitions

    def f_method(_):
        x_val = random() * 2 - 1
        y_val = random() * 2 - 1
        return 1 if x_val ** 2 + y_val ** 2 <= 1 else 0

    count = (
        spark.sparkContext.parallelize(range(1, n_val + 1), partitions)
        .map(f_method)
        .reduce(add)
    )
    print("Pi is roughly %f" % (4.0 * count / n_val))

    spark.stop()
```

### Option 1: Submit Job via CLI

```bash
# Execute the run command in CLI
emrflow serverless run \
        --job-name "emr_serverless_test_job" \
        --entry-point "<location-of-main-python-script>" \
        --spark-submit-parameters " --conf spark.executor.cores=8 \
                                    --conf spark.executor.memory=32g \
                                    --conf spark.driver.cores=8 \
                                    --conf spark.driver.memory=32g \
                                    --conf spark.dynamicAllocation.maxExecutors=100" \
        --s3-code-uri "<emr-s3-path>" \
        --s3-logs-uri "<emr-s3-path>/logs" \
        --execution-timeout 0 \
        --ping-duration 60 \
        --tags "env:dev" \
        --wait \
        --show-output
```

### Option 2: Submit job via API

```python
# import emrflow
import os
from emrflow import emr_serverless

# initialize connection
emr_serverless.init_connection()

# submit job to EMR Serverless
emr_job_id = emr_serverless.run(
    job_name="<job-name>",
    entry_point="<location-of-main-python-file>",
    spark_submit_parameters="--conf spark.executor.cores=8 \
                            --conf spark.executor.memory=32g \
                            --conf spark.driver.cores=8 \
                            --conf spark.driver.memory=32g \
                            --conf spark.dynamicAllocation.maxExecutors=100",
    wait=True,
    show_output=True,
    s3_code_uri="s3://<emr-s3-path>",
    s3_logs_uri="s3://<emr-s3-path>/logs",
    execution_timeout=0,
    ping_duration=60,
    tags=["env:dev"],
)
print(emr_job_id)
```

## Scenario 2: Main Script depends on external libraries not installed in EMR and is Standalone

> [!NOTE]
> EMR only supports core libraries: [emr-release](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-6.x.html). If you have external libraries required for your script, this is the scenario for you.

> [!TIP]
> * Build the dependency package (that only includes library package) using `package-dependencies` command. 
> * Simply submit the job via `run` command


```python
# main.py

import sys
from random import random
from operator import add

# using custom library that's not available in EMR
import pandas as pd
from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
    Usage: pi [partitions]
    """
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n_val = 100000 * partitions

    def f_method(_):
        x_val = random() * 2 - 1
        y_val = random() * 2 - 1
        return 1 if x_val ** 2 + y_val ** 2 <= 1 else 0

    count = (
        spark.sparkContext.parallelize(range(1, n_val + 1), partitions)
        .map(f_method)
        .reduce(add)
    )
    print("Pi is roughly %f" % (4.0 * count / n_val))

    print(f"pandas version is: {pd.__version__}")

    spark.stop()
```

```py
#requirements.txt
pandas==2.0.0
```

### Option 1: Submit Job via CLI

```bash
# package library dependencies
emrflow serverless package-dependencies \
        --output-dir "dist" \
        --package-env \
        --include-paths "requirements.txt" \
        --env-type "conda" \
        --env-python-version 3.8 \
        --env-exec-cmd "pip install -r requirements.txt"

# The command above save library dependencies at 'dist/pyspark_deps.tar.gz'

# execute the run command
emrflow serverless run \
        --job-name <job-name> \
        --entry-point <location-of-main-python-file> \
        --spark-submit-parameters " --conf spark.executor.cores=8 \
                                    --conf spark.executor.memory=32g \
                                    --conf spark.driver.cores=8 \
                                    --conf spark.driver.memory=32g \
                                    --conf spark.dynamicAllocation.maxExecutors=100 \
                                    --conf spark.archives=dist/pyspark_deps.tar.gz#environment \
                                    --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python \
                                    --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python \
                                    --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python" \
        --s3-code-uri "<emr-s3-path>" \
        --s3-logs-uri "<emr-s3-path>/logs" \
        --execution-timeout 0 \
        --ping-duration 60 \
        --tags "env:dev" \
        --wait \
        --show-output
```
### Option 2: Submit job via API
```python
# import emrflow
import os
from emrflow import emr_serverless

# initialize connection
emr_serverless.init_connection()

# package library dependencies
emr_serverless.package_dependencies(
    output_dir="dist",
    package_project=False,
    include_paths=["requirements.txt"],
    package_env=True,
    env_type="conda",
    env_python_version=3.8,
    env_exec_cmd=["pip install -r requirements.txt"],
)

# submit job to EMR Serverless
emr_job_id = emr_serverless.run(
    job_name="<job-name>",
    entry_point="<location-of-main-python-file>",
    spark_submit_parameters="--conf spark.executor.cores=8 \
                            --conf spark.executor.memory=32g \
                            --conf spark.driver.cores=8 \
                            --conf spark.driver.memory=32g \
                            --conf spark.dynamicAllocation.maxExecutors=100 \
                            --conf spark.archives=dist/pyspark_deps.tar.gz#environment \
                            --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python \
                            --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python \
                            --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python",
    wait=True,
    show_output=True,
    s3_code_uri="s3://<emr-s3-path>",
    s3_logs_uri="s3://<emr-s3-path>/logs",
    execution_timeout=0,
    ping_duration=60,
    tags=["env:dev"],
)
print(emr_job_id)
```
## Scenario 3: Main Script is not standalone. Moreover, it requires dependent Python modules from the project, such as src/utils, logging.yaml, config, etc. These additional dependencies are in the project.

> [!TIP]
> - Build the dependency package (that only includes project package) using `package-dependencies` command
> - Simply submit the job via `run` command

```python
# main.py
import sys
from src.pi import get_pi


if __name__ == "__main__":

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    val = get_pi(partitions)
    print("Pi is roughly %f" % val)
```

```python
# src/pi.py

from random import random
from operator import add
from pyspark.sql import SparkSession


def get_pi(partitions: int):
    """
    Usage: pi [partitions]
    """
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()

    n_val = 100000 * partitions

    def f_method(_):
        x_val = random() * 2 - 1
        y_val = random() * 2 - 1
        return 1 if x_val ** 2 + y_val ** 2 <= 1 else 0

    count = (
        spark.sparkContext.parallelize(range(1, n_val + 1), partitions)
        .map(f_method)
        .reduce(add)
    )

    return 4.0 * count / n_val
```

### Option 1: Submit Job via CLI

```bash
# package library dependencies, (--project-dir "src" signifies only package the src folder)
emrflow serverless package-dependencies \
        --output-dir "dist" \
        --package-project \
        --include_paths "src/**"

# execute the run command
emrflow serverless run \
        --job-name deps \
        --entry-point main.py \
        --spark-submit-parameters " --conf spark.executor.cores=8 \
                                    --conf spark.executor.memory=32g \
                                    --conf spark.driver.cores=8 \
                                    --conf spark.driver.memory=32g \
                                    --conf spark.dynamicAllocation.maxExecutors=100 \
                                    --conf spark.submit.pyFiles=dist/project-dependency-src.zip" \
        --s3-code-uri "<emr-s3-path>" \
        --s3-logs-uri "<emr-s3-path>/logs" \
        --execution-timeout 0 \
        --ping-duration 60 \
        --tags "env:dev" \
        --wait \
        --show-output
```
### Option 2: Submit job via API

```python
# import emrflow
import os
from emrflow import emr_serverless

# initialize connection
emr_serverless.init_connection()

emr_serverless.package_dependencies(
    output_dir="dist",
    package_project=True,
    include_paths=["src/**"],
    package_env=False,
)

# submit job to EMR Serverless
emr_job_id = emr_serverless.run(
    job_name="<job-name>",
    entry_point="<location-of-main-python-file>",
    spark_submit_parameters="--conf spark.executor.cores=8 \
                            --conf spark.executor.memory=32g \
                            --conf spark.driver.cores=8 \
                            --conf spark.driver.memory=32g \
                            --conf spark.dynamicAllocation.maxExecutors=100 \
                            --conf spark.submit.pyFiles=dist/project-dependency-src.zip",
    wait=True,
    show_output=True,
    s3_code_uri="s3://<emr-s3-path>",
    s3_logs_uri="s3://<emr-s3-path>/logs",
    execution_timeout=0,
    ping_duration=60,
    tags=["env:dev"],
)
print(emr_job_id)
```
## Scenario 4: The Main Script is not standalone. Additionally, it requires both Python modules and Python libraries in EMR

> [!NOTE]
> EMR only supports core libraries: [emr-release](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-6.x.html). If you have external libraries required for your script, this is the scenario for you.

> [!TIP]
> - Build the dependency package (includes both project and library) using `package-dependencies` command
> - Simply submit the job via `run` command

```python
# main.py

import sys
from src.pi import get_pi


if __name__ == "__main__":

    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    val = get_pi(partitions)
    print("Pi is roughly %f" % val)
```


```python
# src/pi.py

from random import random
from operator import add
from pyspark.sql import SparkSession
import pandas as pd


def get_pi(partitions: int):
    """
    Usage: pi [partitions]
    """
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()

    n_val = 100000 * partitions

    def f_method(_):
        x_val = random() * 2 - 1
        y_val = random() * 2 - 1
        return 1 if x_val ** 2 + y_val ** 2 <= 1 else 0

    count = (
        spark.sparkContext.parallelize(range(1, n_val + 1), partitions)
        .map(f_method)
        .reduce(add)
    )

    print(f"pandas version is: {pd.__version__}")

    return 4.0 * count / n_val
```


```py
#requirements.txt
pandas==2.0.0
```

### Option 1: Submit Job via CLI

```bash
# compile both project and environment dependencies
emrflow serverless package-dependencies \
    --output-dir "dist" \
    --package-project \
    --include_paths "src/**" \
    --include_paths "requirements.txt" \
    --package-env \
    --env-type "conda" \
    --env-python-version 3.8 \
    --env-exec-cmd "pip install -r requirements.txt"

# env dependencies path=dist/project-dependency-src.zip , project dependencies path=dist/pyspark_deps.tar.gz

# submit job to EMR Serverless
# execute the run command
emrflow serverless run \
        --job-name <job-name> \
        --entry-point <location-of-main-python-file> \
        --spark-submit-parameters " --conf spark.executor.cores=8 \
                                    --conf spark.executor.memory=32g \
                                    --conf spark.driver.cores=8 \
                                    --conf spark.driver.memory=32g \
                                    --conf spark.dynamicAllocation.maxExecutors=100 \
                                    --conf spark.archives=dist/pyspark_deps.tar.gz#environment \
                                    --conf spark.submit.pyFiles=dist/project-dependency-src.zip \
                                    --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python \
                                    --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python \
                                    --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python" \
        --s3-code-uri "<emr-s3-path>" \
        --s3-logs-uri "<emr-s3-path>/logs" \
        --execution-timeout 0 \
        --ping-duration 60 \
        --tags "env:dev" \
        --wait \
        --show-output
```

### Option 2: Submit job via API

```python
import os
from emrflow import emr_serverless

# initialize connection
emr_serverless.init_connection()

emr_serverless.package_dependencies(
    output_dir="dist",
    package_project=True,
    include_paths=["src/**", "requirements.txt"],
    package_env=False,
)

# submit job to EMR Serverless
emr_job_id = emr_serverless.run(
    job_name="<job-name>",
    entry_point="<location-of-main-python-file>",
    spark_submit_parameters="--conf spark.executor.cores=8 \
                            --conf spark.executor.memory=32g \
                            --conf spark.driver.cores=8 \
                            --conf spark.driver.memory=32g \
                            --conf spark.dynamicAllocation.maxExecutors=100 \
                            --conf spark.archives=dist/pyspark_deps.tar.gz#environment \
                            --conf spark.submit.pyFiles=dist/project-dependency-src.zip \
                            --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python \
                            --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python \
                            --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python",
    wait=True,
    show_output=True,
    s3_code_uri="s3://<emr-s3-path>",
    s3_logs_uri="s3://<emr-s3-path>/logs",
    execution_timeout=0,
    ping_duration=60,
    tags=["env:dev"],
)
print(emr_job_id)
```
