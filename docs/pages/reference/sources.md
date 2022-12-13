# :material-file: Source file reference

The sources.yml is the file where you store information about your source tables that you want to build data models on top of.

## :material-format-columns: File format options

So far, only yml-files are supported as configuration for source files and other configuration files in Ezt. Ezt also only supports yml-files with the file extension `.yml`.

## :material-format-columns: Layout

The source file has one base key `sources` and under that all of the sources are then gathered as a list. A typical layout with two sources would look something like this:

``` { .yaml title="sources.yml" .annotate }
sources: #(1)
  - name: usa_cars #(2)
    filesystem: local #(3)
    path_type: file #(4)
    format: parquet #(5)
    path: /home/SomeUser/data/usa_cars.parquet
    

  - name: car_sales
    filesystem: s3
    path_type: folder
    format: parquet
    path: my-bucket/car_sales
```

1. This key is **required** and the value of it is always a list.

2. This key is **required** and the value can be basically anything. The name specified here is used in models when calling the `get_source()` -function.

3. This key is **required** and is used to specify the filesystem on which the specific source is stored.

4. This key is **required** and is used to tell Ezt if the path (specified in the value of the `path:` key) is a fodler or a file.

5. This key is **required** and is used to tell Ezt what kind of format the data is stored as.

## :material-format-columns: Supported filesystems

Ezt currently supports sources in the **local filesystem** and in **AWS S3**. If there is a concrete need from the user base, other filesystems such as Azure ADLS Gen 2 and Google Cloud Storage might be supported in the future.

A source in Ezt needs different configuration settings depending on the filesystem where it is stored.

## :material-format-columns: Supported file types

Ezt currently supports Parquet and CSV as file types for sources.
