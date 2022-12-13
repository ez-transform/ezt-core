# Project structure

When initiating an Ezt-project, it will automatically create some files and folder for you. The structure of simple Ezt project will look something like this:

``` { .sh .annotate }
/myproject
|——— __init__.py
|——— .gitignore
|——— README.md
|——— sources.yml #(1)
|——— ezt_project.yml #(2)
|——— models #(3)
|    |——— __init__.py
|    |——— models.yml #(4)
|    |——— my_model.py #(5)
|    |——— staging_models #(6)
|    |   |——— __init__.py
|    |   |——— models_staging.yml
|    |   |——— stage_1.py
|    |   |——— stage_2.py
```

1. This is where you put your sources.

2. This file contains various project-specific configurations. Such as the folder structure for models inside the project and if logging should be enabled or not.

3. This is the default folder in which you create your model yml-configurations and your python files.

4. This is where your model configurations go.

5. This is where your model code go.

6. You can also put your models into sub-folders to be able to structure what code goes where better.

## Sources

Sources always go inside the `sources.yml` file and should be located in the base directory of your project. There are plans to add functionality that would allow users to structure sources better in the future.

## ezt_project.yml

The `ezt_project.yml` currently determines the following things:

* Name of project.
* Structure of models.
* Logging destination.

### Example ezt_project.yml

``` { .yaml title="ezt_project.yml" .annotate }
name: 'my_project' #(1)

models: #(2)
  main_folder: models #(3)
  groups: #(4)
    - name: sales
    - name: products

logs_destination: /home/john/ezt-test/logs #(5)
```

1. Name of project

2. The models-key tells Ezt how models are structured.

3. `main_folder` points to the base folder of your model-structure.

4. `groups` tells Ezt how many folders containing models that are inside the main_folder. The names specified here has to match with the folder names.

5. Path to where execution logs should be stored.

## Models

As seen from the ezt_project.yml example above, models can be structured into a hierarchy of folders with 2 levels, the main_folder and unlimited number of groups inside that. Each model-folder has to contain:

<div class="annotate" markdown>
+ \__init__.py
+ models*.yml some text after
+ One python-file per model defined in models.yml.
</div>

!!! note annotate "models.yml in model groups"

    The file containing model configuration can in a model group be named `models*.yml`, where the star can be any word or character. This allow the user to distinguish between model.yml files if they are working with many. For example in a group named *sales*, the yml-file can be named for example `models_sales.yml`.
