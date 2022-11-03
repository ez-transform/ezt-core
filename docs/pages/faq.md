# Frequently asked questions (FAQ)

## 1. Can I write my models as SQL?

> At the moment, SQL is not supported in Ezt. However there are plans to support SQL in the future to lower the barrier of entry for engineers not familiar with dataframe manipulation.

## 2. Why is Polars preferred instead of a more popular dataframe library, such as Pandas or Spark?

> Polars has been chosen as the main data manipulation library since it supports paralell execution and is very memory efficient, which allows users to work with much larger datasets and achieve a lot faster processing speeds compared to Pandas.

> Spark is a distributed analytics engine, meaning that it is meant to run on clusters instead of a single machine. The goal of Ezt is not to act as a substitute to Spark, but rather as a compliment for use cases where the distributed capabilities of Spark are not needed.

## 3. How much data can I process with Ezt?

> **A lot**. But it all depends on the machine/server on which you are running your models. One of the goals of Ezt is to allow you to run the majority of your data engineering pipelines on low-cost services such as serverless container services. In the future, we hope to provide some benchmarks so that the user is able to better make decisions on what infrastructure to choose for running Ezt.
