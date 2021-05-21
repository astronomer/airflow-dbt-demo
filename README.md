# Airflow DAGs for dbt

> The code in this repository is meant to accompany [this blog post](https://astronomer.io/blog/airflow-dbt-1) on
> beginner and advanced implementation concepts at the intersection of dbt and Airflow.

## To run these DAGs locally:

1. Download the [Astro CLI](https://github.com/astronomer/astro-cli)
2. Download and run [Docker](https://docs.docker.com/docker-for-mac/install/)
3. Clone this repository and `cd` into it.
4. Run `astro dev start` to spin up a local Airflow environment and run the accompanying DAGs on your machine.
5. **Run the `dbt_basic` DAG first** as this contains a `dbt seed` command that loads sample data into the database. You can then
run the other two DAGs any time after without having to load the data again.

## dbt project setup

We are currently using [the jaffle_shop sample dbt project](https://github.com/fishtown-analytics/jaffle_shop). 
The only files required for the Airflow DAGs to run are `dbt_project.yml`, `profiles.yml` and 
`target/manifest.json`, but we included the models for completeness. If you would like to try these DAGs with 
your own dbt workflow, feel free to drop in your own project files.

**Note** that the sample dbt project contains the `profiles.yml`, which is configured to use Astronomer's 
containerized postgres database **solely for the purpose of this demo**. In a production environment, you should use a 
production-ready database and use environment variables or some other form of secret management for the database 
credentials.
