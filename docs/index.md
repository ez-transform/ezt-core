# Welcome to ezt-docs

:octicons-squirrel-16: ez-transform (Ezt) - Analytics engineering for data lakes.

## :octicons-light-bulb-24: Concept

`ezt` aims to provide a powerful and simple analytics engineering experience (similar to [dbt](https://www.getdbt.com/)) for building data models on a data lake, a *Data Lakehouse* if you will, without having to use a third-party processing engine to execute the computations. Ezt is powered by [polars](https://github.com/pola-rs/polars), [arrow](https://github.com/apache/arrow) and [delta-rs](https://github.com/delta-io/delta-rs). Ezt provides you with:

* Standardized project template
* Possibility to use fully-fledged software development practices, such as version control, CI/CD, unit testing etc.
* CLI-based interface for easy use in CI/CD pipelines
* Freedom to choose your own compute engine
* Delta-lake support through delta-rs

Take a look at the [Guides](pages/guides/installation.md)-section in order to learn how to install Ezt and build your first models.
