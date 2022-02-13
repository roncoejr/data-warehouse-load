# data-warehouse-load

Code utility that can be used to load a dataset into a data warehouse

## Introduction

## Inspiration

## Setup

### The AUTH directory

In the case of Google and Amazon properties; you have the opportunity to retrieve .json files that represent your authentication credentials.  Should you choose to go this route it is intended that you'd store those files in an AUTH directory--which is listed in the .gitignore so that those files aren't placed in source control.

### Environment Variables

Whether you're authenticating with .json files or not, environment variables are a necessity for all of the platforms you'll use in this manner.  These variables will either hold the authentication credentials directly or they'll point to the .json file holding the creds.

#### Snowflake

- SNOW_URL
- SNOW_ACCOUNT
- SNOW_USER
- SNOW_PASSWORD

#### Google BigQuery

- GOOGLE_APPLICATION_CREDENTIALS

#### Amazon Redshift

#### Azure Synapse Analytics
