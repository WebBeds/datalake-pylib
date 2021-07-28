# **Datalake Python Library**

Private repository for store and maintain python packages used in different repositories of Datalake.

---

## Requirements

In order to utilize the packages that contain this repository you need to install **gpip** with **pip**, the recommended version is the 0.3.7.

```bash
pip3 install gpip==0.3.7
```

---

## **Stable packages**

You can view the current stable and older versions of a package in this repository [**here**](https://github.com/Webjet/datalake-pylib/tags).

The version of the packages will follow this pattern. **package-version**. Example: **etl-schema-0.1**.

The versions can be specified in the gpip installation, as the #version flag, where you can specify the version, this will checkout the tag specified.

---

## Example of installation of a package.

This will install the [Schema package](https://github.com/Webjet/datalake-pylib/tree/main/etl/schema) of the **etl** group.

```
gpip get github.com/Webjet/datalake-pylib@etl.schema#name=datalake-etl.etl-schema
```

---

## Issue

If you have an issue you can publish [**here**](https://github.com/Webjet/datalake-pylib/issues)