# **Datalake Python Library**

Private repository for store and maintain python packages used in different repositories of Datalake.

---

## Requirements (Deprecated)

In order to utilize the packages that contain this repository you need to install **gpip** with **pip**, the recommended version is the 0.4.7

```bash
pip3 install gpip==0.4.7
```

---

## **Stable packages**

You can view the current stable and older versions of a package in this repository [**here**](https://github.com/Webjet/datalake-pylib/tags).

The version of the packages will follow this pattern. **package-version**. Example: **etl-schema-0.1**.

The versions can be specified in the gpip installation, as the **==** flag similar to **pip**, where you can specify the version, this will checkout the tag specified.

---

## **Current packages**

The packages are separated by groups.

<details>
    <summary>
        <b>ETL</b> Library <span style="margin-left:5px;font-size:9px">Utility packages of Datalake.</span>
    </summary>
    <ul>
        <li>
            <h5>
                <a href="https://github.com/Webjet/datalake-pylib/tree/main/etl/schema">
                    <b>Schema</b> <b style="color:lightblue">WIP</b>
                </a>
            </h5>
            <p style="font-size:10px">
                Normalize dataframes.
            </p>
        </li>
        <li>
            <h5>
                <a href="https://github.com/Webjet/datalake-pylib/tree/main/etl/pandas">
                    <b>Pandas</b><sub style="color:red"><b>EARLY</b></sub>
                </a>
            </h5>
            <p style="font-size:10px">
                Make actions with Pandas DataFrames like getting reports from differences between two DataFrames.
            </p>
        </li>
        <li>
            <h5>
                <a href="https://github.com/Webjet/datalake-pylib/tree/main/etl/s3">
                    <b>S3</b><sub style="color:red"><b>EARLY</b></sub>
                </a>
            </h5>
            <p style="font-size:10px">
                Manage and make action on S3 with Pandas DataFrames.
            </p>
        </li>
        <li>
            <h5>
                <a href="https://github.com/Webjet/datalake-pylib/tree/main/etl/database">
                    <b>Database</b><sub style="color:red"><b>EARLY</b></sub>
                </a>
            </h5>
            <p style="font-size:10px">
                Interact with AWS Athena or Postgres, send queries or get dataframes.
            </p>
        </li>
        <li>
            <h5>
                <a href="https://github.com/Webjet/datalake-pylib/tree/main/etl/aws">
                    <b>AWS</b><sub style="color:red"><b>EARLY</b></sub>
                </a>
            </h5>
            <p style="font-size:10px">
                Some usefull AWS methods that let your code be more dynamically.
                Example, detect when the machine that is running your code is a Lambda function.
            </p>
        </li>
        <li>
            <h5>
                <a href="https://github.com/Webjet/datalake-pylib/tree/main/etl/utils">
                    <b>Utils</b><sub style="color:red"><b>EARLY</b></sub>
                </a>
            </h5>
            <p style="font-size:10px">
                Some usefull utilities without a common property but are utilized in the ETL repository. Example, send alarm to teams, loggin utility for making prints more complex.
            </p>
        </li>
    </ul>
</details>

---

## Example of installation of a package.

This will install the [Schema package](https://github.com/Webjet/datalake-pylib/tree/main/etl/schema) of the **etl** group.

```
gpip get github.com/Webjet/datalake-pylib/etl/schema:datalake-etl-schema
```

---

## Issue

If you have an issue you can publish [**here**](https://github.com/Webjet/datalake-pylib/issues)
