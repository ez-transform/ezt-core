# Registering sources

The main way to use Ezt is to already have some data located somewhere, on top of which you want to build your data models. In order for Ezt to understand how to fetch and read that data, you need to list information about that source inside the `sources.yml` file, which . This guide will show you how to register a source.

To register a source, you need to have knowledge about the following:

* Location, such as filesystem and file/folder path.
* File type
* Authentication (for remote object stores, like S3)

The way to set up a source slightly differs depending on the filesystem and file type.

Currently Ezt only supports parquet as filetype for sources in S3. In the future, the plan is to support delta-tables as well.

The yml definition of a source in S3 can look something like this:

``` { .yaml title="sources.yml" .annotate }
sources:
  - name: new_cars #(1)
    filesystem: s3 #(2)
    path_type: folder #(3)
    format: parquet #(4)
    path: my-bucket/data/new_cars #(5)
```

1. Every source requires a unique name.

2. Filesystems supported right now are `local` filesystem and `S3`.

3. `path_type` can be either `file` or `folder`. When set to `file`, the `path` needs to point to a specific file, and when set to `folder`, the `path` needs to point to a folder and every file in that folder will get included in the dataset.

4. File-format in which the data is stored. Only parquet is supported at the moment for sources in S3.

5. `path` points to a specific file or folder depending on the `path_type` value.

So in this example, we have defined a source named `new_cars` residing in S3 and this is all you need to do in order to register a source. To later use this source inside a data model, you need to have `$AWS_ACCESS_KEY_ID` and `$AWS_SECRET_ACCESS_KEY` set as environment variables from the shell that runs the `ezt run` command.

Next, check out the guide for creating a model to find out how to reference this source dataset inside a data model.
