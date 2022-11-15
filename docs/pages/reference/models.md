# :material-file: Model reference

The models.yml is the file where you store information about how your models should be written to storage.

## :material-format-columns: File format options

So far, only yml-files are supported as configuration for model configuration files. Ezt also only supports yml-files with the file extension `.yml`.

## :material-format-columns: Layout

The source file has one base key `models` and under that all of the sources are then gathered as a list. A typical layout with two models would look something like this:

``` { .yaml title="models.yml" .annotate }
models: #(1)
  - name: products_model #(2)
    type: df #(3)
    filesystem: s3 #(4)
    destination: ezt-target-dev #(5)
    write_settings: #(6)
      file_type: parquet #(7)
      mode: overwrite #(8)
  
  - name: total_car_sales
    type: df
    filesystem: local
    destination: /home/john/ezt-test/destination_data
    write_settings:
      file_type: delta
      mode: merge
      key: order_id
```

1. This key is **required** and the value of it is always a list.

2. This key is **required** and the value can be basically anything. The name specified here is used when calling the `get_model()` -function from another model to chain models.

3. This key is **required** and the value should be `df`. Sql models will be supported in the future as well.

4. This key is **required** and is used to specify the filesystem on which the specific model should be created.

5. This key is **required** and is used to tell Ezt the path to where the models should be stored.

6. This key is **required** and the value is an object with the keys `file_type` and `mode`.

7. This key is **required** and tells ezt how to persist the model. For S3-models, only parquet is supported at the moment. Local filesystem models supports ´parquet´ and ´delta´.

8. This key is **required** and tells ezt if processed data should be appended, overwritten or merged. `merge` requires an additional key under `write_settings` named `merge_key` to specify on which column the merge-logic should be calculated.

## :material-format-columns: Supported filesystems

Ezt currently supports models in the **local filesystem** and in **AWS S3**. If there is a concrete need from the user base, other filesystems such as Azure ADLS Gen 2 and Google Cloud Storage might be supported in the future.

## :material-format-columns: Supported file types

Ezt currently supports Parquet and delta as file types for models. For S3-models only parquet is currently supported.
