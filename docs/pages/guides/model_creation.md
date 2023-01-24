# Creating a model

The main way to create a model in Ezt, is to have existing datasets somewhere that are defined in your Ezt project as sources and build a data model on top of those where the datasets get joined, aggrated, filtered, pivoted etc.

Ezt also allows *chaining* of models. This means that existing models can function as input for other models. When using the built-in `get_source` and `get_model` functions to fetch datasets in your model, Ezt automatically calculates the correct order in which to execute the models when calling `ezt run` from the command line. If one model depends on another model, Ezt first processes and persists the first model in the chain, and then proceeds to process and persist the second model.

Models without dependencies on other models gets processed in parallell. To achieve this, Ezt uses the concept of a directed acyclical graph (DAG), which get calculated at runtime, in order to determine which models can be processed in parallell and which models need to be processed before another model.

## Model definition

In order to create a model, the user needs to define the model in a `models.yml` file (similar to how sources are defined) and then create a `.py` -file containing a function named `df_model` that returns a Polars DataFrame. It is entirely up to the user to define the logic that creates that DataFrame, either by putting all logic inside the `df_model` function, or using other functions or classes defined somewhere else in the project, to handle the calculations producing the DataFrame that then gets returned by the `df_model` function.

Let's first look at the yml-configuration.

### Defining the model configuration

The model configuration tells Ezt the type of the model and how the model should be persisted.

A `models.yml` can look something like this:

``` { .yaml title="models.yml" .annotate }
models:
  - name: product_metrics #(1)
    filesystem: s3 #(2)
    type: df #(3)
    destination: my-ezt-bucket/product #(4)
    write_settings: #(5)
      file_type: parquet #(6)
      mode: overwrite #(7)
```

1. Just like a source, a model requires a unique name. **Required key**

2. Filesystems supported right now are `local` filesystem and `s3`. **Required key**

3. Ezt currently only supports DataFrame models defined in python. There are plans to also support SQL models in the future. **Required key**

4. Destination tells Ezt where to store the model. This path should always be pointing to a folder. **Required**

5. Write settings is a yml dictionary that handles how the model should be persisted. **Required key**

6. Currently, `parquet` and `delta` are supported as format for storing models. **Required key**

7. The mode can be either `overwrite`, `append` or `merge`. **Required key**

#### Modes

Ezt supports the modes `append`, `overwrite` or `merge`.

* Overwrite simply overwrites the existing model with the new result.
* Append adds all rows of the new result to the existing result.
* When `mode` is set to *merge*, an additional key named *merge_col* is needed to be able to calculate which rows should be updated and which rows should be inserted. At the moment, deletes are not supported and needs to be handled entirely by the user. For example, having a column in your dataset set to either True or False depending on if the given row is deleted or not is a good strategy.

### Defining the python model

When you have created the model definition and decided where and how to persist your data, you can start building your model. The model should be created in a python file named `model_name.py` (where *model_name* is the name of your model defined in `models.yml`) and return a Polars DataFrame. Polars is installed as a dependency to Ezt so no need to install it explicitly.

The function returning the Polars DatFrame should be decorated with `@py_model` and be named `df_model`.

A model can look something like this:

``` { .py title="product_metrics.py" .annotate }
from ezt import py_model, get_source

@py_model
def df_model():

    products_lf = get_source('products_raw')
    sales_lf = get_source('sales_raw')

    query = (
        products_lf
        .join(sales_lf, on='ProductKey', how='inner')
        .select(
            [
                pl.col("ProductKey"),
                pl.col("ProductName"),
                pl.col("OrderQuantity"),
                pl.col("ProductCost"),
                pl.col("ProductPrice"),
                (pl.col("ProductPrice") - pl.col("ProductCost"))
                    .alias("UnitMargin"),
                (pl.col("ProductPrice") * pl.col("OrderQuantity"))
                    .alias('TotalSalesAmount'),
            ]
        )
    )

    return query.collect()

```

In this model, two sources named "products_raw" and "sales_raw" are first fetched by using the `get_source` -function. Beware that the `get_source` and `get_model` functions return a Polars LazyFrame and not a DataFrame. The `query` then performs a join and then a select where two additional columns get calculated and added to the result. Since the computations get done on LazyFrames, we need to call `collect()` on the result before returning it to return a DataFrame.

If Polars syntax is unfamiliar, you can check out the [Polars API docs](https://pola-rs.github.io/polars/py-polars/html/reference/) and the [Polars User Guide](https://pola-rs.github.io/polars-book/user-guide/) to learn more.

In practice, one can use any library they want to perform the computations on the data, as long as the df_model function returns a Polars DataFrame. Polars is currently one of the fastest and most memory efficient DataFrame libraries existing for single machines and is therefore recommended. Source: [h2oai's db-benchmark](https://h2oai.github.io/db-benchmark/).

Next, check out the guide for how to structure your project.
