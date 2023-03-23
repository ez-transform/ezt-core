# Analyzing data

A drawback of a data lake in comparison to a data warehouse, is the lack of a query interface. There is no built-in functionality through which you could easily query and analyze a particular dataset located in a data lake. Teams usually solve this either by using spark or some kind of query engine, like AWS Athena.

Ezt tries to solve this problem by letting users utilize the `get_source` and `get_model` to analyze data in for example notebooks without having to write all the necessary code to read a specific file type from a specific filesystem. The `get_source` and `get_model` can be called from any working directory by specifying the optional parameter `project_base_dir`, which should be a path to the directory where your `ezt_project.yml` is located.

For example, you could have a `.notebooks` -folder inside your ezt-project with notebooks that contains data analysis on your sources and models.

This is an example how to use the `get_source` function to read a source you have defined in your `sources.yml` located in some remote storage supported by Ezt (currently S3 and Azure ADLS Gen2).

``` py
from ezt import get_source, get_model
from dotenv import load_dotenv

# load environment variables from .env -file for authentication to remote storage
load_dotenv()

# fetch source by name defined in source.yml
my_lazyframe = get_source(name="my_source", project_base_dir = "../")

```

After that you can query your data however your want.