# DBT Documentation
## DBT Env setup
- Create a separate venv or conda env (Ensure you are using python 3.10)
  - conda create -n dbt python=3.10
- Install requirements.txt (dbt is included in this --> [Docs](https://docs.getdbt.com/docs/core/pip-install)):
  - pip install -r requirements.txt
- Install dbt dependencies:
  - dbt deps


## Generating and Serving DBT Docs
- Activate your dbt env
  - This depends on your setup
    - For example: ```conda acivate py_dbt```
- Port-forward to Hive
  - Port: 10000
  - You can do this via K9s or some other tool against the hive2 pod in the cluster
- Ensure you are in the dbt directory within hush-sound
  - .../hush-sound/transformations/dbt
- Run the following command to generate the docs:
  - ``` dbt docs generate ```
    - NOTE: If you receive this error:
    - ``` Expected only one database in get_catalog, found [<InformationSchema INFORMATION_SCHEMA>, <InformationSchema INFORMATION_SCHEMA>]```
    - Go to the dbt_project.yml file and comment this line: ```database: true```
  - This command generates the following docs:
    - catalog.json
    - graph.gpickle
    - index.json
    - manufest.json 
    - run_results.json
- Run the following command to serve up the docs in a broswer
  - ```dbt docs serve```
  - This will open the dbt model documentation in your brower on 8080

## Running dbt commands
- Ensure you are in the hush-sound/transformations/dbt directory

- Run full-refresh of a model
  - ```dbt run --profiles-dir . --select <NAME_OF_MODEL> --full-refresh```
- Run a model
  - ```dbt compile --profiles-dir . --select <NAME_OF_MODEL>```
- Run models of a certain tag
  - ```dbt run --profiles-dir . --select tag:<NAME_OF_TAG> --full-refresh```


## DBT Tagging 

TODO 
- Add info about the tagging strategy
    - Staged
    - Denormalized
    - Load_to_warehouse
    - Archive

## Common Issues
- Cannot find table (raw or otherwise)
  - Make sure your 'PIPELINE_ENV' environment variable is set correctly
  



---
# ARCHIVE

We are no longer testing through dbt. This is handled via airflow for now. We may revisit this later on once we move tests and checks more upstream
07/27/2023 Nathan M
## DBT Tests
### Testing Strategy

In general, dbt tests want to follow the Trusted Data Framework (TDF)
1. Freshness monitors Monitor for unusual delays in table and field updates
2. Schema monitors Monitor fields that are added, removed or changed
3. Volume monitors Monitor for unusual changes in table size based on the numbers of rows
4. Field health Monitor Monitor fields for dips or spikes in stats like % null, % unique, and more. Our ML sets the thresholds.
5. SQL rule monitor Write a SQL statement to check for any expressible condition across 1 or more tables in your data.
6. JSON schema monitor Monitor for schema changes in JSON data added to a table field.
7. Dimension tracking Monitor for changes in the distribution of values within a low-cardinality table field.
8. Schema tests to validate the <ins>**integrity of a schema**</ins>
9. Column Value tests to determine <ins>**if the data value in a column matches pre-defined thresholds or literals**</ins>
10. <ins>**Rowcount tests**</ins> to determine if the number of rows in a table over a pre-defined period of time match pre-defined thresholds or literals
11. Golden Data tests to determine if <ins>**pre-defined high-value data exists**</ins> in a table
12. Custom SQL tests any valid SQL that doesn't conform to the above categories


Monitors (1-7 above) should be executed via a third party solution as outlined [here](https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#trusted-data-framework).

For tests numbered 8 through 12, schema and column value tests will usually be in the main project. These will be in schema.yml and sources.yml files in the same directory as the models they represent.


**Outdated Need to Fix**

**Adding Tests**
1. Navigate to dbt/aux_scripts/test_map.py
2. Add test to test_map.json either in table or cols (table being a test against a table and cols being a test against a column). Below is the format for adding a test

```

<REGEX_PATTERN>:
    {
        "description": <TEST_DESCRIPTION>,
        "tests" : {
            <TEST_1> : {
                <PARAM_1>: <VALUE_1>,
                <PARAM_2>: <VALUE_2>
              },
            <TEST_2> : {}
        },
        "exceptions" : <LIST_OF_TABLE_OR_COL_EXCEPTIONS>
    }
```
Below is an example of the test format:
```
"\\w*etag": 
    {
        "description" : "Checking that etag is not null",
        "tests" : {
            "not_null" : {}},
        "exceptions" : []
    },
```


3. The steps for adding a test are as follows:
    1. Add the regex pattern. If needed, here is a [regex tester](https://regex101.com/)
    2. Add the test(s) that will be applied to the tables or columns that match the regex pattern
    3. Add the test parameters (if any) as key-value pairs 
4. Rerun dbt_schema_generator to apply the new test to the tables and/or columns
5. Run ``` dbt tests --profiles-dir . ``` to test dbt models with new or modified tests



### Reference
- [Gitlab Guide to DBT](https://about.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide)
- [DBT Test Reference Doc](https://docs.getdbt.com/reference/resource-properties/tests)
- [DBT Building Tests Doc](https://docs.getdbt.com/docs/build/tests)


